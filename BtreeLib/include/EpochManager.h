
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
* The implmentation is designed to be non-intrusive in the sense that it doesn't
* use any thread-local variables. In other words, threads accessing a data structure
* that uses this epoch manager are entirely unaware of the epoch manager. For example,
* a thread doesn't need to "register" with the epoch manager.
*
* The implementation is not fully lock free - the operation of advancing the epoch
* is protected by an exclusive lock. A thread stalling while holding the lock does
* not block other threads from making progress but it increases memory consumption
* by blocking physically dealloction - the deallocation list of the current epoch
* just keeps growing. 
* TODO: Implement epoch advancement using an multiword CAS operation
*       which would make it lock free.
*
* Paul Larson, gpalarson@outlook.com, October 2016
* ================================================================================= */

#pragma once

#define ASSERT_WITH_TRACE(condition, msg, ...) _ASSERTE(( L ## #condition ## msg, condition));

// Signature of the finalize callback function. 
// The epoch manger calls this function on an item when  it is safe to garbage collect.
//
typedef __callback void (*EpochFinalizeCallback)(
    __in void* callbackObject,
    __in void* objectToFinalize, 
    __in MemObjectType );

// Exclusive lock with no waiting. If a thread fails to acquire the lock it just continues on.
// Used in one place only: advancing an epoch is protected by an exclusive lock.
//
class ExclusiveLock
{
  enum LockState:ULONG {UNLOCKED, LOCKED};

  volatile ULONG	  m_Lock;

public:
  // Constructor
  ExclusiveLock() { m_Lock = UNLOCKED; }

  // Try to acquire the lock. Returns true if lock was acquired, false otherwise
  bool TryAcquireLock()
  {
	ULONG resVal = InterlockedCompareExchange(&m_Lock, LOCKED, UNLOCKED);
	return (resVal == UNLOCKED);
  }

  // Release the lock unconditionally
  void ReleaseLock()
  {
	StoreWithRelease<volatile ULONG>(&m_Lock, UNLOCKED);
  }

};

// Node representing an item waiting for garbage collection.
//
class GCItem
{
public: 
  // Next item on the lock-free linked list
  GCItem*		m_NextItem;

  // Used for tracking memory usage by item type
  MemObjectType m_ItemType;

  // Data object to physically delete
  void*			m_pDataObject;


  GCItem(MemObjectType type=MemObjectType::Invalid)
	: m_NextItem(nullptr), m_ItemType(type), m_pDataObject(nullptr)
  {}
};

// An epoch manager protects objects from premature deallocation by an epoch mechanism.
// An object freed by the application is not immediately deallocated but
// kept in a limbo list until it is guaranteed that no thread has a reference
// to the object any longer. More specifically, if an object was freed during an epoch E 
// then it can be safely deallocated when epoch E has been closed (no threads can enter E any longer) 
// and all threads that entered epoch E have exited E.
//
class EpochManager
{

public:
    static const __int64 s_InvalidEpochId = -1;

private:
    
    // Stores the data required to manage an epoch. The manager uses only
    // two Epoch nodes to manage garbage collection (current and next).
    struct Epoch
    {
        Epoch()
           :m_DeallocationList(nullptr), 
			m_LastItemHint(nullptr),
            m_nMemberCount(0),
            m_nItemCount(0)
        {}

        // Garbage list for this epoch. Once this epoch has drained and no
        // threads access any objects on the list, the list is moved to the
        // central garbage list where garbage collection is parallelized across threads.
 	    GCItem* m_DeallocationList;

		// This field almost always points to the last item on the garbage list. 
		// It does not, if the item it points to has a non-null next pointer. In that case,
		// we must scan the complete garbage list to locate the last item. This is very rare.
		GCItem* m_LastItemHint;
        
		// Number of items in this epoch's deallocation list.
        volatile LONG64 m_nItemCount;

        // Number of threads that are currently members of this epoch.
		volatile LONG64 m_nMemberCount;
    };

public:
    EpochManager();
    ~EpochManager();

	// An epoch manager must be initialized before it can be used.
    __checkReturn HRESULT Initialize( __in MemoryBroker* pMemoryBroker, __in void* pvFinalizeContext, __in EpochFinalizeCallback finalizeCallback);

	// and un-initialized when no longer needed
    __checkReturn HRESULT UnInitialize();

	// A thread calls these functions to enter and exit an epoch.
    __checkReturn HRESULT EnterEpoch( __out __int64* pnEpochId, __in_opt bool bIsReader = false);
    __checkReturn HRESULT ExitEpoch( __in __int64 nEpochId, __in_opt bool bIsReader = false);

	// Function called by a thread to free an object. The actual deallocation happens later when it's safe to do so.
    __checkReturn HRESULT Deallocate(__in void* pvDeallocObject, __in MemObjectType);

	// Function called by a thread to free an object and immediately deallallocate it.
    __checkReturn HRESULT DeallocateNow(__in void* pvMemoryToFree, __in MemObjectType type = MemObjectType::Invalid);

	// Threads call this function when they participate in deallocating objects found on the central GC list.
    __checkReturn HRESULT DeallocateOnFinalize(__in void* pvMemoryToFree, __in MemObjectType type);


private:

	HRESULT MakeGCItem(__in void* pvDataItem, __in MemObjectType type, __out GCItem** ppGCNode);
   
	// Atomically push a list of GCItems or a single GCItem onto a deallocation queue (limbo list).
	static void PushOntoQueue(__in GCItem** pQueueHead, GCItem* pNewFirst, GCItem* pNewLast)
	{
	  _ASSERTE(pQueueHead && pNewFirst && pNewLast);
	  _ASSERTE(((UINT64(pNewFirst) % MEMORY_ALLOCATION_ALIGNMENT) == 0));
	  _ASSERTE(((UINT64(pNewLast) % MEMORY_ALLOCATION_ALIGNMENT) == 0));

	  if (pQueueHead && pNewFirst && pNewLast)
	  {
		LONG64 resVal = 0;
		GCItem* pFirst = nullptr;
		do
		{
		  pFirst = *pQueueHead;
		  pNewLast->m_NextItem = pFirst;
		  resVal = InterlockedCompareExchange64((LONG64*)(pQueueHead), LONG64(pNewFirst), LONG64(pFirst));
		}while (resVal != LONG64(pFirst));

	  }
	}

	// Atomically pop a GCItem from the deallocation queue (limby list)
	static GCItem* PopFromQueue(GCItem** pQueueHead)
	{
	  GCItem* pFirst = nullptr;
	  GCItem* pNext = nullptr;
	  LONG64 resVal = 0;
	  do
	  {
		pFirst = *pQueueHead;
		if (pFirst == nullptr) break;

		pNext =  pFirst->m_NextItem ;
		resVal = InterlockedCompareExchange64((LONG64*)(pQueueHead), LONG64(pNext), LONG64(pFirst));
	  } while ( resVal != LONG64(pFirst));

	  return pFirst;
	}

    __checkReturn HRESULT TryAdvanceEpoch();

    __checkReturn HRESULT DoDeallocationWork(__in int itemDeallocationCount);
     __checkReturn HRESULT DeallocateItem(__in GCItem* pItem);
	 __checkReturn HRESULT MigrateDeallocationQueue(__in EpochManager::Epoch* pEpochEntry);
 
	 // Functions for advancing and reading epoch numbers.
	__checkReturn __int64 GetNextEpochValueInternal() { return (m_nCurrentEpoch + 1) % EpochManager::EpochCount; }
    __checkReturn __int64 GetNextEpochValueExternal() { return (m_nCurrentEpoch + 1); }
    __checkReturn __int64 GetCurrentInternalEpoch()   { return m_nCurrentEpoch % EpochManager::EpochCount; }
    __checkReturn __int64 GetCurrentExternalEpoch()	  { return m_nCurrentEpoch; }
	__checkReturn __int64 TranslateToInternalEpoch(__in __int64 nExternalEpochValue) { return (nExternalEpochValue % EpochManager::EpochCount);	}
  

    static const ULONG	 EpochCount = 2;
    static const ULONG	 EpochAdvanceThreshold = 30;
    static const __int64 DrainQueueDeallocCount = -1;
    static const __int64 DeallocCountLarge = 50;
    static const __int64 DeallocCountSmall = 20;

	
	bool			  m_IsReadyForUse;						// Flag indicating whether the manager is ready for use or not.
	ExclusiveLock	  m_AdvancementLock;					// Exclusive lock to allow only one thread to advance an epoch (no waiters).

	LONG64			  m_nCurrentEpoch;						// The current epoch.
	Epoch			  m_epochs[EpochManager::EpochCount];	// Array of epoch structures 

	GCItem*			  m_CentralDeallocationList;			// Linked list of items that are safe to to garbage collect. Shared across worker threads.
    volatile LONG64	  m_nCentralQueueSize;					// The number of items currently in the central deallocation queue.
    
	EpochFinalizeCallback m_finalizeCallback;				// Callback function used to finalize epoch-managed objects.
    void*				  m_pFinalizeContext;				// Context passed to the finalize callback function.
 
	volatile __int64   m_nMemoryAllocatedCountInGC;			// Currently allocated memory in bytes of objects on the garbage list.

	// All dynamically objects are acquired and freed through this memory broker.
	// It tracks memory usage by object type (but doesn't know about objects in GC)
	MemoryBroker*		m_pMemoryBroker;
 
};

