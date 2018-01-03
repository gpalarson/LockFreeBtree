/* =================================================================================
* The files EpochManager.h and EpochManager.cpp contain an implementation of a simple
* epoch manager. The manager delays the actual deallocation of a memory object 
* until there are no longer any references to the object. If an application deallocates
* and object during an epoch E, it is safe to physically deallocate the object
* when all threads active during epoch E has exited the epoch.
*
* Each epoch has two frequently updated global variables that may become bottlenecks.
* 1. A membership counter which is updated when a thread enters or exits an epoch.
* 2. The head of the deallocation list which is updated whenever an object is deallocated.
* If the update rate of one of these variables is sufficiently high to significantly
* reduce performance, the problem can be remedied by paritioning the counters and/or
* the deallocation lists. 
*
* The implementation is not fully lock free - the operation of advancing the epoch
* is protected by an exclusive lock. A thread stalling while holding the lock does 
* not block other threads from makng progress but it increases memory consumption
* by blocking physically dealloction - the deallocation list of the current epoch
* just keeps growing. 
* TODO: ImplementeEpoch advancement using an multiword CAS operation
*       which would make it lock free.
*
* Paul Larson, gpalarson@outlook.com, October 2016
* ================================================================================= */
#include <Windows.h>
#include <crtdbg.h>
#include "Utilities.h"
#include "MemoryBroker.h"
#include "EpochManager.h"


// Create a GCItem to be pushed onto an epoch's deallocation list
// Returns an error if memory allocation fails.
//
HRESULT EpochManager::MakeGCItem( 
  __in void* pvDataItem,			// item to add to the deallocation list
  __in MemObjectType type,			// item type
  __out GCItem** ppGCNode)			// pointer to the new GCItem
{
    if(!ppGCNode) return E_POINTER;
    *ppGCNode = nullptr;

    // Allocate the entry through the aligned allocator interfaces.
    GCItem* pGCNode;
    HRESULT hr = m_pMemoryBroker->AllocateAligned( sizeof(GCItem), MEMORY_ALLOCATION_ALIGNMENT , (void**)(&pGCNode),
	                                             MemObjectType::GCItemObj);
    if(FAILED(hr)) return hr;
	_ASSERTE(((UINT64(pGCNode) % MEMORY_ALLOCATION_ALIGNMENT) == 0));

	pGCNode->m_NextItem = nullptr;
    pGCNode->m_pDataObject    = pvDataItem;
	pGCNode->m_ItemType = type;
    *ppGCNode = pGCNode;

    return hr;
}

// Constructor
EpochManager::EpochManager()
    :
  m_IsReadyForUse(false),
    m_nCurrentEpoch(0),
    m_nMemoryAllocatedCountInGC(0),
    m_pFinalizeContext(nullptr),
    m_finalizeCallback(nullptr),
	m_CentralDeallocationList(nullptr),
    m_nCentralQueueSize(0)
{}


// Destructor
EpochManager::~EpochManager()
{
  HRESULT hr = UnInitialize();
  if (FAILED(hr))
  {
	ASSERT_WITH_TRACE(false, "Epoch mananer uninitialize failed");
  }
}


// An epoch manager must be proeprly initialized before it can be used.
// Return values: S_OK successfully initialized, E_POINTER input argument is NULL
//
__checkReturn HRESULT EpochManager::Initialize(
    __in MemoryBroker* pMemoryBroker,				// Memory broker to use for allocating GCItems (broker tracks memory usage)
    __in void* pFinalizeContext,					// Context to pass to the finalize callback function
    __in EpochFinalizeCallback finalizeCallback)	// Function to call when finalizing (deallocating) a memory object
{
    if(!pMemoryBroker || !pFinalizeContext) return E_POINTER;

    m_pMemoryBroker = pMemoryBroker;
    m_pFinalizeContext = pFinalizeContext;
    m_finalizeCallback = finalizeCallback;
	m_IsReadyForUse = true;

    return S_OK;
}


// Uninitialize an epoch manager and dellocate all items on its garbage list.
// The manager can be deallocated after successful return of this function.
// Return values:
// S_OK			uninitialize completed successfully
// E_UNEXPECTED unexpected state found in epoch (e.g., membership count non-zero in one of the  epochs).
// Other		error codes from deallocator
//
__checkReturn HRESULT EpochManager::UnInitialize()
{
    // Clear the central dealloc list
    HRESULT hr = DoDeallocationWork(EpochManager::DrainQueueDeallocCount);
    if(FAILED(hr)) return hr;

    // Clear the epoch dealloc lists.
    for(int nEpochNdx = 0; nEpochNdx < EpochManager::EpochCount; ++nEpochNdx)
    {
        GCItem* pCurrentNode = PopFromQueue(&m_epochs[nEpochNdx].m_DeallocationList);
        while(pCurrentNode)
        {
            ::InterlockedDecrement64(&m_epochs[nEpochNdx].m_nItemCount);

            // Finalize the data in the current GCItem and deallocate the item itself.
            hr = DeallocateItem(pCurrentNode);
            if(FAILED(hr)) return hr;

            // Get the next deallocation item in the list.
			pCurrentNode = PopFromQueue(&m_epochs[nEpochNdx].m_DeallocationList);
        }

        if(m_epochs[nEpochNdx].m_nItemCount > 0)
        {
            ASSERT_WITH_TRACE(false, "Epoch not empty!");
            return E_UNEXPECTED;
        }
    }

    // Reset member variables.
    m_nCurrentEpoch = 0;
    m_pFinalizeContext = nullptr;
    m_finalizeCallback = nullptr;
	m_IsReadyForUse = false;

    return hr;
}


// Attempt to enter the current epoch. Successfully joining the epoch guarantees
// that all objects touched by the thread will remain stable until it exits the epoch.
// Return values: 
// S_OK - Epoch successfully joined. Thread can proceed with processing.
// E_POINTER Null input argument.
// Function can also return errors returned by calls to allocated/deallocator.
//
__checkReturn HRESULT EpochManager::EnterEpoch(
    __out __int64* pnEpochId,			// where to return (external) epoch id 
    __in_opt bool bIsReader)			// readers do not participate in maintenance work
{
    if(!pnEpochId) return E_POINTER;
    *pnEpochId = EpochManager::s_InvalidEpochId;

    HRESULT hr = S_OK;

    Epoch* pCurrentEpoch = &(m_epochs[GetCurrentInternalEpoch()]);
    if(!bIsReader && pCurrentEpoch->m_nItemCount > EpochManager::EpochAdvanceThreshold)
    {
        hr = TryAdvanceEpoch();
        if(FAILED(hr)) return hr;
        else 
		  if(S_FALSE == hr)
		  {
			  // S_FALSE means we were not able to advance the epoch because
			  // (a) we need to wait for the next epoch to drain or 
			  // (b) we were not chosen as the advancer. 
			  // Either way, we can just continue on.
			  hr = S_OK;
		  }
    }

	__int64 nExternalEpochId = EpochManager::s_InvalidEpochId;
    __int64 nInternalEpochId = EpochManager::s_InvalidEpochId;
     bool bMembershipSuccess = false;
    while(!bMembershipSuccess)
    {
        // Become a member of the current epoch.
        nExternalEpochId = GetCurrentExternalEpoch();
        nInternalEpochId = TranslateToInternalEpoch(nExternalEpochId);

        ::InterlockedIncrement64(&(m_epochs[nInternalEpochId].m_nMemberCount));

        if(nInternalEpochId != GetCurrentInternalEpoch())
        {
            // The epoch we entered is no longer the current one. Leave the old epoch and retry.
            ::InterlockedDecrement64(&(m_epochs[nInternalEpochId].m_nMemberCount));
            bMembershipSuccess = false;
        }
        else
        {
            bMembershipSuccess = true;
        }
    }

	// We just entered so memmbership count must be greater than zero
    if(m_epochs[nInternalEpochId].m_nMemberCount <= 0)
    {
        ASSERT_WITH_TRACE(false, "member count <= 0");
        return E_UNEXPECTED;
    }

    if(!bIsReader)
    {
        // Try to do some work to dealloc from previous epoch. 
        hr = DoDeallocationWork(EpochManager::DeallocCountLarge);
        if(FAILED(hr)) return hr;
    }

    *pnEpochId = nExternalEpochId;
    return hr;
}


// Exit the epoch identified by nEpochId. After successfull exit any memory objects
// touched by the thread will be unstable and may disappear at any time.
// Return values 
// S_OK			  successful exit
// E_INVALIDARG	  invalid nEpochId value
// Function may also return error based on calls to allocated/deallocator when
//  trying to advance the internal epoch counter.
//
__checkReturn HRESULT EpochManager::ExitEpoch(
    __in __int64 nEpochId,						// (external) id of epoch to leave
    __in_opt bool bIsReader)					// readers do not participate in maintenance work
{
    if(nEpochId == EpochManager::s_InvalidEpochId || nEpochId < 0) return E_INVALIDARG;

    __int64 nEpochIdToExit = TranslateToInternalEpoch(nEpochId);
    ::InterlockedDecrement64 (&(m_epochs[nEpochIdToExit].m_nMemberCount));

	// Member count should never be negative
    __int64 nMemberCount = m_epochs[nEpochIdToExit].m_nMemberCount;
    if(nMemberCount < 0)
    {
        ASSERT_WITH_TRACE(nMemberCount >= 0, "epoch member count < 0");
        return E_UNEXPECTED;
    }

    Epoch* pCurrentEpoch = &(m_epochs[GetCurrentInternalEpoch()]);
    HRESULT hr = S_OK;

    if(!bIsReader &&
       pCurrentEpoch->m_nItemCount > EpochManager::EpochAdvanceThreshold)
    {
        hr = TryAdvanceEpoch();
        if(FAILED(hr)) return hr;
        else 
		  if(S_FALSE == hr)
		  {
			  // S_FALSE means we were not able to advance the epoch because
			  // (a) we need to wait for the next epoch to drain or 
			  // (b) we were not chosen as the advancer. 
			  // Either way, just continue on
			  hr = S_OK;
		  }
    }

    return hr;
}


// Attempt to advance the current epoch. Epoch advancement is single-threaded.
// The thread that wins the exclusion is responsible for advancing the epoch
// value. Threads that do not gain exclusion simply backoff and proceed with processing.
// **** TODO: need to remove this lock to make the epoch mechanism lock free. ****
// Return values:
// S_OK successfully advanced the epoch
// S_FALSE epoch could not advance due to active members in the candidate epoch 
//         or some other thread is holding the advancement lock
// E_UNEXPECTED unexpected state observed (e.g., epoch advanced past the worker).
// Function can also return error based on call migrate the central garbage list.
//
__checkReturn HRESULT EpochManager::TryAdvanceEpoch()
{
    //CComWriteLock writeLock(m_AdvancementLock, FALSE);

	HRESULT hr = S_FALSE;

    // Attempt to get the write lock in exclusive mode.  If this fails, then
    // someone else is already advancing the epoch.
 	if( !m_AdvancementLock.TryAcquireLock())
    {
		// We didn't acquire the lock so just exit
        return hr;
    }

	// We now have the lock - don't forget to release it

    // The previously used epoch structure will be used for the next epoch.
    __int64 nCurrentExternalEpochValue = GetCurrentExternalEpoch();
    __int64 nNextEpochIdInternal       = GetNextEpochValueInternal();
    Epoch* pNextEpoch = &(m_epochs[nNextEpochIdInternal]);

    // We can only advance the epoch if the previous epoch has no members
	// becasue the previous epoch becomes the new current epoch
	// when the epoch is advanced (we have only two epoch slots).
    if(pNextEpoch->m_nMemberCount > 0)
    {
        goto exit;
    }

    // Previous epoch has no members, migrate its queue to the central
    // deallocation queue.
    hr = MigrateDeallocationQueue(pNextEpoch);
	if (FAILED(hr))
	{
	  goto exit;
	}

    // We can safely advance the epoch now.
    if(nCurrentExternalEpochValue != GetCurrentExternalEpoch())
    {
        ASSERT_WITH_TRACE(false, "unequal epoch values");
        hr = E_UNEXPECTED;
		goto exit;
    }

    // No need for an atomic op here. This thread has write exclusion to
    // m_nCurrentEpoch, so just use StRel.
    __int64 nNextEpochIdExternal = GetNextEpochValueExternal();
    StoreWithRelease<__int64>(&m_nCurrentEpoch, nNextEpochIdExternal);

    if(nNextEpochIdExternal != m_nCurrentEpoch)
    {
        ASSERT_WITH_TRACE(false, "unexpected epoch value");
        hr = E_UNEXPECTED;
		goto exit;
    }

  exit:
	m_AdvancementLock.ReleaseLock();
    return hr;
}



// Deallocate an item under epoch protection. This functions enqueues an item
// for deletion. The item is not actually freed until no threads can possibly
// dereference it any longer.
// Return values:
// S_OK		Epoch successfully advanced.
// S_FALSE	Tried to but failed to advance epoch due to active members in the candidate epoch.
// E_POINTER Null argument passed to function.
// E_UNEXPECTED Unexpected state - unable to add item to deallocation list.
// Function can also return error based on calls to allocator.
//
__checkReturn HRESULT EpochManager::Deallocate(
  __in void* pvDeallocObject,					// object to deallocate
  __in MemObjectType type)						// type of the object
{
	// Check args
	if (pvDeallocObject == nullptr) return E_POINTER;
	_ASSERTE(type >= MemObjectType::First && type <= MemObjectType::Last);

   // Get the current epoch.
    Epoch* pCurrentEpoch = &(m_epochs[GetCurrentInternalEpoch()]);

	// Create a GCItem owning the object
    GCItem* pDeallocNode = nullptr;
    HRESULT hr = MakeGCItem(pvDeallocObject, type, &pDeallocNode);
    if(FAILED(hr)) return hr;

	// Add the itme to the deallocation queue
	PushOntoQueue(&pCurrentEpoch->m_DeallocationList, pDeallocNode, pDeallocNode);
	if (pCurrentEpoch->m_LastItemHint == nullptr)
	{
	  pCurrentEpoch->m_LastItemHint = pDeallocNode;
	}
 
	// Update counters
	ULONG nSize = 0;
	ULONG nGCSize = 0;
	hr = m_pMemoryBroker->GetAllocatedSize(pvDeallocObject, &nSize);
 	hr = m_pMemoryBroker->GetAlignedAllocatedSize(pDeallocNode, &nGCSize);
    ::InterlockedIncrement64(&pCurrentEpoch->m_nItemCount);
	::InterlockedExchangeAdd64(&m_nMemoryAllocatedCountInGC, (__int64)(nSize + nGCSize));


    // Look for dealloc work to do from previous epoch(s).
    hr = DoDeallocationWork(EpochManager::DeallocCountSmall);
    if(FAILED(hr)) return hr;

    // Try to advance epoch if deallocation queue grows too large.
    if(pCurrentEpoch->m_nItemCount > EpochManager::EpochAdvanceThreshold)
    {
        HRESULT hr = TryAdvanceEpoch();
        if(FAILED(hr)) return hr;
    }

    return hr;
}


// Help along in peforming deallocation work from the central deallocation
// list. Attempt to perform nDeallocateCount deallocations. All work items are
// guaranteed to be safe for deallocation, i.e., they cannot be derefenced by
// any thread in the system.
// Function may return error based on call to manipulate the central garbage
//
__checkReturn HRESULT EpochManager::DoDeallocationWork(
  __in int nDeallocationCount)							// how many items to deallocate
{
    __int64 nCurrDeallocCount = 0;
 	HRESULT hr = S_OK;
	
    GCItem* pNodeToDeallocate = PopFromQueue(&m_CentralDeallocationList);
    while(pNodeToDeallocate)
    {
        hr = DeallocateItem(pNodeToDeallocate);
        if(FAILED(hr)) break;

        ++nCurrDeallocCount;
 
        if(nDeallocationCount > 0 && nCurrDeallocCount >= nDeallocationCount)
        {
            // The worker did its allotment of drain work - time to exit
            //
            break;
        }        

		pNodeToDeallocate = PopFromQueue(&m_CentralDeallocationList);
    }
    ::InterlockedAdd64(&m_nCentralQueueSize, -nCurrDeallocCount);


    return hr;
}

// Migrate the deallocation queue for the given epoch to the central shared work list.
// Return values
// S_OK		  garbage list for the epoch successfully migrated.
// E_POINTER  Null argument passed to function.
// Function can also return error based on calls to manipulate the central garbage
//
__checkReturn HRESULT EpochManager::MigrateDeallocationQueue( __in EpochManager::Epoch* pEpoch)
{
    if(!pEpoch) return E_POINTER;
	HRESULT hr = S_OK;

	GCItem* pFirst = pEpoch->m_DeallocationList;
	if (pFirst)
	{
	  // LastItmeHint may not be correct. If it isn't, scan the list to locate the last item.
	  if( pEpoch->m_LastItemHint == nullptr || pEpoch->m_LastItemHint->m_NextItem != nullptr)
	  {
		GCItem* pcur = pFirst;
		for ( /* nothing */; pcur != nullptr && pcur->m_NextItem != nullptr; pcur = pcur->m_NextItem);
		pEpoch->m_LastItemHint = pcur;
	  }
	  // At this point m_LastItemHint must point to the last entry of the deallocation queue.
	  _ASSERTE(pEpoch->m_LastItemHint != nullptr && pEpoch->m_LastItemHint->m_NextItem == nullptr);
	  PushOntoQueue(&m_CentralDeallocationList, pFirst, pEpoch->m_LastItemHint );
        
	  ::InterlockedAdd64(&m_nCentralQueueSize, pEpoch->m_nItemCount);

	  pEpoch->m_DeallocationList = nullptr;
	  pEpoch->m_LastItemHint = nullptr;
	  pEpoch->m_nItemCount = 0;

	}

    return hr;
}

// Perform actual deallocation for an item by calling the finalize callback for the given GCItem.
// Reutrn values
// S_OK		 item successfully deallocated.
// E_POINTER Null argument passed to function.
// Function may also return errors based on calls to allocator.
//
__checkReturn HRESULT EpochManager::DeallocateItem(__in GCItem* pItem)
{
    if(!pItem) return E_POINTER;

    if(m_finalizeCallback ) m_finalizeCallback(m_pFinalizeContext, pItem->m_pDataObject, pItem->m_ItemType);

   HRESULT hr = m_pMemoryBroker->FreeAligned(pItem, MEMORY_ALLOCATION_ALIGNMENT, MemObjectType::GCItemObj);
 
	return hr;
}

// Deallocate a memory object immediately. Assumes that the object
// is safe for immediate deallocation and does not require epoch protection. 
// Return values
// S_OK deallocation finished successfully.
// E_POINTER Null input argument.
// E_UNEXPECTED Unexpected epoch manager state encountered 
// Function can also return error from call to memory allocator.
//
__checkReturn HRESULT EpochManager::DeallocateNow(
  __in void* pvMemoryToFree,	  // memory object to free
  __in MemObjectType type)		  // object type
{
    if(!pvMemoryToFree) return E_POINTER;

    if(!m_pMemoryBroker)
    {
        ASSERT_WITH_TRACE(false, "null memory allocator");
        return E_UNEXPECTED;
    }

  	HRESULT hr = m_pMemoryBroker->Free(pvMemoryToFree, type);
    return hr;
}

// Deallocate the given memory object. Used for functions that are
// finalizing an object. 
// Return values
// S_OK  Deallocation finished successfully.
// E_POINTER Null input argument.
// E_UNEXPECTED Unexpected epoch manager state - no reference to memory broker.
// Function can also return error from call to  memory allocator.
//
__checkReturn HRESULT EpochManager::DeallocateOnFinalize(
  __in void* pvMemoryToFree,	// memory object to deallocate
  __in MemObjectType type)		// object type
{
    if(!pvMemoryToFree) return E_POINTER;

    if(!m_pMemoryBroker)
    {
        ASSERT_WITH_TRACE(false, "null memory allocator");
        return E_UNEXPECTED;
    }

    HRESULT hr = m_pMemoryBroker->Free(pvMemoryToFree, type);
    if(FAILED(hr)) return hr;

    return hr;
}

