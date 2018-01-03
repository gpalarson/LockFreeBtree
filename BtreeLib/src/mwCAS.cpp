
// ***************************************************************************
// The files mwCAS.h and mwCAS.cpp contain an implementation of a 
// multi-word compare-and-swap (MWCAS) operation. The operation is 
// lock free and non-blocking. It requires a flag bit in each word
// that participates in an MWCAS operation (an MwCasTargetField). 
// It is primarily designed for atomically swapping 64-bit pointers  
// but can be used for any data type of size 63 bits or less.
// 
// The implmentation uses a preallocated, partitoned pool of MWCAS
// descriptors and relies on reference counting to protect access
// to descriptors. 
// 
// To perform an MWCAS operation, the user first allocates a descriptor and
// initializes it with target addresses, old values, and new values and then 
// calls mwCAS() on the descriptor. The return value indicates whether the 
// operations succeded or not.
// 
// The implementation is based on ideas in the following paper.
//    Harris, Timothy L., Fraser, Keir, Pratt, Ian A., 
//    A Practical Multi-word Compare-and-Swap Operation", DISC 2002, 265-279
//
//  Paul Larson, May 2016, gpalarson@outlook.com
// ***************************************************************************
#include <Windows.h>
#include <stdio.h> 
#include <assert.h>
#include "mwcas.h"

// Global pool of descriptors. 
static MwCasDescriptorPool g_MwCASDescriptorPool;

MwCASDescriptor* AllocateMwCASDescriptor(ULONG flagPos)
{
    return g_MwCASDescriptorPool.AllocateMwCASDescriptor(flagPos);
}

void MwCasStats::AddCounts(MwCasCounts* partCounts)
{
  if (partCounts)
  {
	for (UINT32 l = 0; l < s_MaxStatsDepth; l++)
	{
	  Counts[l].m_Attempts += partCounts[l].m_Attempts;
	  Counts[l].m_Bailed   += partCounts[l].m_Bailed;
	  Counts[l].m_Succeded += partCounts[l].m_Succeded;
	  Counts[l].m_Failed   += partCounts[l].m_Failed;
	  Counts[l].m_HelpAttempts += partCounts[l].m_HelpAttempts;
	}
  }
}

void MwCasStats::PrintStats()
{
  for (UINT32 l = 0; l < s_MaxStatsDepth; l++)
  {
	if (Counts[l].m_Attempts > 0)
	{
	  printf("          %2d %10dA,%6.2f%% %10dB,%4.2f%% %10dS,%4.2f%% %10dF,%4.2f%% %10dH,%4.2f%%\n", l,
		Counts[l].m_Attempts, 100.0*double(Counts[l].m_Attempts) / Counts[0].m_Attempts,
		Counts[l].m_Bailed, 100.0*double(Counts[l].m_Bailed) / Counts[l].m_Attempts,
		Counts[l].m_Succeded, 100.0*double(Counts[l].m_Succeded) / Counts[l].m_Attempts,
		Counts[l].m_Failed, 100.0*double(Counts[l].m_Failed) / Counts[l].m_Attempts,
		Counts[l].m_HelpAttempts, 100.0*double(Counts[l].m_HelpAttempts) / Counts[l].m_Attempts);
	}
  }
}

void MwCasDescriptorPool::PrintMwCasStats()
{
  MwCasStats sumStats;

  sumStats.InitStats();

  for (UINT32 i = 0; i < m_PartitionCount; i++)
  {
	sumStats.AddCounts(m_PartitionTbl[i].m_StatsCounts);
  }
  sumStats.PrintStats();
}

MwCasDescriptorPartition::MwCasDescriptorPartition(MwCasDescriptorPool* ownerPool, UINT preallocate)
{
  m_MwDescrPool = ownerPool;
 

  for (UINT32 l = 0; l < s_MaxStatsDepth; l++)
  {
	m_StatsCounts[l].InitCounts();
  }

  // Preallocate a small number of descriptors for each partition
  m_MwDescrFreeList = nullptr;  
  m_DescCount = 0;
  for (UINT k = 0; k < preallocate; k++)
  {
	MwCASDescriptor* desc = (MwCASDescriptor*)(_aligned_malloc(sizeof(MwCASDescriptor), CACHE_LINE_SIZE));
	if (!desc) break;
	
	new(desc) MwCASDescriptor(this);

	PushOntoQueue(desc);
	m_DescCount++;
	m_MwDescrPool->m_DescInPool++;
  }

}

MwCasDescriptorPartition::~MwCasDescriptorPartition()
{
  MwCASDescriptor* cur = PopFromQueue();
  while (cur)
  {
	_aligned_free(cur);
	cur = PopFromQueue();
  }
  m_MwDescrPool = nullptr;
  for (UINT32 i = 0; i < s_MaxStatsDepth; i++)
  {
	m_StatsCounts[i].InitCounts();
  }
}


// The MWCAS descriptor pool must be initialized before starting to allocate descriptors.
MwCasDescriptorPool::MwCasDescriptorPool(UINT partitions, UINT preallocate )
{
	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	UINT numCPU = sysinfo.dwNumberOfProcessors;

	// Round nr of partitions to a power of two but no higher than 1024
	m_PartitionCount = 1;
	for (UINT exp = 1; exp < 10; exp++)
	{
		if (partitions <= m_PartitionCount) break;
		m_PartitionCount *= 2;
	}

	m_DescInPool = 0;
	m_PartitionTbl = (MwCasDescriptorPartition*)( _aligned_malloc(sizeof(MwCasDescriptorPartition)*m_PartitionCount, CACHE_LINE_SIZE));
	assert(m_PartitionTbl);

    // Initially allocate per partition at most as many descriptors as CPUs
    UINT descCount = min(preallocate, numCPU);
	for (UINT i = 0; i < partitions; i++)
	{
	  new(&m_PartitionTbl[i]) MwCasDescriptorPartition(this, descCount);
	  m_DescInPool += descCount ;
	}
	
	m_MaxPoolSize = max(4 * numCPU, 2 * m_DescInPool);

#ifdef _DEBUG
    fprintf(stdout, "Descriptor pool initialized\n");
    fprintf(stdout, "%d partitions, %d descriptors each of size %d bytes\n", m_PartitionCount, descCount, INT(sizeof(MwCASDescriptor)));
#endif
}

MwCasDescriptorPool::~MwCasDescriptorPool()
{
  m_MaxPoolSize = 0;
  m_DescInPool = 0;
  for (UINT32 i = 0; i < m_PartitionCount; i++)
  {
	m_PartitionTbl[i].~MwCasDescriptorPartition();
  }
}

// Get a free MwCASDescriptor from the pool.
MwCASDescriptor* MwCasDescriptorPool::AllocateMwCASDescriptor(ULONG flagPos)
{
  _ASSERTE(flagPos < 64);

  UINT slot = HashThreadId(GetCurrentThreadId()) & (m_PartitionCount - 1);
  MwCASDescriptor* desc = nullptr;
  do {
	desc = m_PartitionTbl[slot].AllocateMwCASDescriptor();
	if (!desc)
	{
	  // Allocate space for a new descriptor but don't exceed pool size limit
	  if (m_DescInPool >= m_MaxPoolSize) break;

	  MwCASDescriptor* newdesc = (MwCASDescriptor*)(_aligned_malloc(sizeof(MwCASDescriptor), CACHE_LINE_SIZE));
	  if (!newdesc) break;	  // Out of memory

	  new(newdesc) MwCASDescriptor(&m_PartitionTbl[slot]);
	  m_PartitionTbl[slot].ReleaseMwCASDescriptor(newdesc);
	  m_DescInPool++;
	}
  } while (!desc);

  if (desc)
  {
	desc->m_Count = 0;
	desc->m_FlagBitMask = (UINT64)(1) << flagPos;
    ;

	_ASSERTE(desc->m_DescStatus.Status32 == MwCASDescriptor::FINISHED);
	_ASSERTE(desc->m_DescStatus.RefCount == ~MwCASDescriptor::RefCountMask + 0);
  }

  return desc;
}


// Return a descriptor to the pool
void MwCASDescriptor::ReturnDescriptorToPool()
{
  _ASSERTE(m_OwnerPartition);
  m_OwnerPartition->ReleaseMwCASDescriptor(this);
}


MwCASDescriptor::MwCASDescriptor(MwCasDescriptorPartition* owner)
  :MwCasDescriptorBase(MWCAS_DESCRIPTOR)
{
  m_DescStatus.RefCount = ~RefCountMask + 0 ;
  m_DescStatus.Status32 = FINISHED;
  m_OwnerPartition = owner;
  m_FlagBitMask = 0;
  m_Count = 0;
}

LONGLONG CondCASDescriptor::CondCASRead(LONGLONG* addr, UINT64 typeMask)
{
  _ASSERTE(addr);
  LONGLONG rval = 0;
  bool isDesc = false;
  ULONG count = 0;
  CondCASDescriptor* desc = nullptr;

  do
  {
	count++;
	rval = *addr;
	isDesc = IsCondCASDescriptor(rval, typeMask);
	if (isDesc)
	{
	  desc = (CondCASDescriptor*)ClearDescriptorFlag(rval, typeMask);
	  desc->CompleteCondCAS();
	}
	if (count > 10000) break;
  } while (isDesc);

  _ASSERTE(!isDesc);

  return rval;
}

LONGLONG CondCASDescriptor::CondCAS(LONGLONG& finalVal, bool& ignoreResult)
{
  LONGLONG retVal = 0;

  if (!m_OwnerDesc->SecureAcces())
  {
	ignoreResult = true;
	return retVal;
  }

 
  // At this point we know that the descriptor is active and
  // can't be made inactive because it has a non-zero ref count
  ignoreResult = false;

  LONGLONG descptr = LONGLONG(SetDescriptorFlag(this, m_OwnerDesc->m_FlagBitMask));


  // First phase: try to swap in a pointer to this CondCAS descriptor 
  do
  {
	retVal = InterlockedCompareExchange64(m_TargetAddr, descptr, m_OldVal);
	if (IsCondCASDescriptor(retVal, m_OwnerDesc->m_FlagBitMask))
	{
	  CondCASDescriptor* desc = (CondCASDescriptor*)(ClearDescriptorFlag(retVal, m_OwnerDesc->m_FlagBitMask));
	  desc->CompleteCondCAS();
	}

  } while (IsCondCASDescriptor(retVal, m_OwnerDesc->m_FlagBitMask));

  // If the first phase succeeded, complete the operation 
  // by swapping in NewValue. If it didn't succeed, try to restore the old value.

  finalVal = (retVal == m_OldVal) ? CompleteCondCAS(true) : retVal;
 
  m_OwnerDesc->ReleaseAccess();

  return retVal;
}

// Swap in the new value (pointer to the MwCas descriptor) if the descriptor state is still UNDECIDED,
// otherwise restore the old value.
LONGLONG CondCASDescriptor::CompleteCondCAS( bool hasAccess)
{
  if (!MwCASDescriptor::IsHelpingAllowed(m_OwnerDesc->m_DescStatus.RefCount))
  {
	return 0;
  }

  if (!hasAccess)
  {
	bool secured = m_OwnerDesc->SecureAcces();
	if (!secured)
	{
	  // The descriptor is INACTIVE so the return value doesn't matter.
	  return 0;
	}
  }

  // Determine what value to swap in
  LONGLONG tmpval  = LONGLONG(SetDescriptorFlag(m_OwnerDesc, m_OwnerDesc->m_FlagBitMask));
  LONGLONG replVal = (m_OwnerDesc->m_DescStatus.Status32 == MwCASDescriptor::UNDECIDED) ? tmpval : m_OldVal;

  // Update the target word. The first phase of the CondCAS operation set the target
  // word to point to the CondCAS descriptor (witht the flag bit set).
  // If the CAS fails, some other thread has already helped the operation along.
  LONGLONG expVal = LONGLONG(SetDescriptorFlag(this, m_OwnerDesc->m_FlagBitMask));
  LONGLONG actVal = InterlockedCompareExchange64(m_TargetAddr, replVal, expVal);

  // Return the current (final) value of the target word
  LONGLONG finalVal = (actVal == expVal) ? replVal : actVal;
 
  if (!hasAccess)
	m_OwnerDesc->ReleaseAccess();

  return finalVal;
}

// Called before helping to complete an MwCAS operation.
// Returns true if helping is allowed and also increases the descriptor's refcount. 
// Returns false if helping is not allowed.
bool MwCASDescriptor::SecureAcces()
{
  UINT32 curRefCount = 0;
  UINT32 newRefCount = 0;
  UINT32 retRefCount = 0;
 
  while (true)
  {
	curRefCount = m_DescStatus.RefCount;
	if (!IsHelpingAllowed(curRefCount ))
	{
	  return false;
	}


	newRefCount = curRefCount + 1;
	retRefCount = InterlockedCompareExchange((LONG*)(&m_DescStatus.RefCount), newRefCount, curRefCount);
	if (retRefCount == curRefCount)
	{
	  // Success - we can now safely access the descriptor
	  return true;
	}

	// No success but did we fail because helping is not allowed?
	if (!IsHelpingAllowed(retRefCount ))
	{
	  return false;
	}
  }

  return true;
}


// Decrements the ref count and returns the descriptor to the pool if refcount goes to zero.
void MwCASDescriptor::ReleaseAccess()
{

  MwCASDescriptor::uStatus curStatus = m_DescStatus;
  MwCASDescriptor::uStatus newStatus = curStatus;
  MwCASDescriptor::uStatus retStatus;

  do
  {
	curStatus = m_DescStatus;

	newStatus = curStatus;
	newStatus.RefCount--;
	if (curStatus.RefCount == 1)
	{
	  _ASSERT(curStatus.Status32 == MwCASDescriptor::SUCCEEDED || curStatus.Status32 == MwCASDescriptor::FAILED);
	  m_SavedOutcome = m_DescStatus.Status32;
	  newStatus.Status32 = FINISHED;            // MwCAS operation is finished
	  newStatus.RefCount = ~RefCountMask + 0;   // so disallow helping
	}
	retStatus.Status64 = InterlockedCompareExchange64((LONGLONG*)(&m_DescStatus.Status64), newStatus.Status64, curStatus.Status64);
  } while (retStatus.Status64 != curStatus.Status64);

  // If we release the last reference, we must return the descriptor to the pool for reuse.
  if (curStatus.RefCount == 1)
   {
	ReturnDescriptorToPool();
  }

}



// Word descriptors are stored sorted on the address to prevent livelocks.
// Returns the number of word descriptors included in the MWCAS operation.
// Return value is negative if the descriptor is full or the address is a duplicate.
INT32 MwCASDescriptor::AddEntryToDescriptor( __in LONGLONG* addr, __in LONGLONG oldval, __in LONGLONG newval)
{
  _ASSERTE(!IsDescriptorPtr(oldval, m_FlagBitMask));
  _ASSERTE(!IsDescriptorPtr(newval, m_FlagBitMask));
  //_ASSERTE(UINT64(addr) % sizeof(LONGLONG) == 0);

	INT32 retvalue = -1 ; 
	if( m_Count < MaxWordsPerDescriptor)
	{
		int insertpos = m_Count ;
		for( int i=m_Count-1; i>=0; i--)
		{
			if (m_CondCASDesc[i].m_TargetAddr == addr)
			{
				// Can't allow duplicate addresses because it makes the desired result of 
				// the operation ambigous. If two different new values are specified for the
				// same address, what is the correct result?
				// Also, if the operation fails we can't guarantee that the old values will be 
				// correctly restored.
				return -2;
			}
			if( m_CondCASDesc[i].m_TargetAddr < addr )
			{
				break ;
			}
			m_CondCASDesc[i+1] = m_CondCASDesc[i] ;
			insertpos-- ;
		}

		m_CondCASDesc[insertpos].m_Type      = CONDCAS_DESCRIPTOR;
		m_CondCASDesc[insertpos].m_OwnerDesc = this;
		m_CondCASDesc[insertpos].m_TargetAddr  = addr ;
		m_CondCASDesc[insertpos].m_OldVal    = oldval ;
		m_CondCASDesc[insertpos].m_NewVal    = newval;
		m_Count++ ;
		retvalue = m_Count ;
	}
	return retvalue ;
} 

// Executes the MWCAS operation specified by the descriptor.
// Note that multiple threads may be executing this function concurrently.
bool MwCASDescriptor::MwCAS(UINT calldepth)
{
	MwCasCounts* stats  = &m_OwnerPartition->m_StatsCounts[min(calldepth,s_MaxStatsDepth)];	  

	stats->m_Attempts++;

	uStatus  curStatus;
	uStatus  newStatus;
	ULONG threadId = GetCurrentThreadId();

	if (calldepth == 0)
	{
		// Called from the owning thread which is the only one that can release the operation for processing
		m_SavedOutcome = UNDECIDED;
		do
		{
			curStatus = m_DescStatus;
			assert(curStatus.Status32 == FILLED);
			newStatus = curStatus;
			newStatus.RefCount++;
			newStatus.RefCount = (newStatus.RefCount & RefCountMask); // Clear flag to allow helping
			newStatus.Status32 = UNDECIDED;
		} while (curStatus.Status64 !=
			InterlockedCompareExchange64((LONGLONG*)(&m_DescStatus.Status64), newStatus.Status64, curStatus.Status64));

	} else 
	{

		// Called from a helping thread. Bail out if the operation is not in the first or the second phase.
		do
		{
			curStatus = m_DescStatus;
			if (!IsHelpingAllowed(curStatus.RefCount))
			{
				// The MwCAS operation has been completed so no help is needed.
				// The return value is ignored when returning from helping so just return true.
				stats->m_Bailed++;
				return true;
			}

			// Operation is in first or second phase so help complete it.
			newStatus = curStatus;
			newStatus.RefCount++;
		} while (curStatus.Status64 !=
			InterlockedCompareExchange64((LONGLONG*)(&m_DescStatus.Status64), newStatus.Status64, curStatus.Status64));

	}





	// In the first phase we attempt to set every target word to point to the current descriptor
	// with the flag bit set to indicate that the word contains a descriptor pointer.
	LONGLONG descptr = LONGLONG(SetDescriptorFlag(this, m_FlagBitMask))  ;
	
	if( m_DescStatus.Status32 == UNDECIDED)
	{
		// PHASE ONE ----------------------------

		eDescState newStatus = SUCCEEDED;
		bool  ignoreResult = false;

		// Try to swap a pointer to this descriptor into all target addresses
		for( int i=0; i < m_Count && (newStatus == SUCCEEDED); i++)
		{

tryagain:

		  LONGLONG rval = 0;
		  LONGLONG ph1Result = 0;


		  CondCASDescriptor* cdesc = &m_CondCASDesc[i];

		  rval = cdesc->CondCAS(ph1Result, ignoreResult);
		  if (ignoreResult)
		  {
			goto tryagain;
		  }



		  // Do we need to help another MWCAS operation?
		  if(IsMwCASDescriptor(rval, m_FlagBitMask))
		  {
			if (ClearDescriptorFlag(rval, m_FlagBitMask) != this)
			{

			  // Clashed with another MWCAS; help complete the other MWCAS if it is still being worked on	
			  MwCASDescriptor* otherMWCAS = (MwCASDescriptor*)(ClearDescriptorFlag(rval, m_FlagBitMask));

			  if (otherMWCAS != nullptr )
			  {
				bool helpRes = otherMWCAS->MwCAS(calldepth + 1);
				stats->m_HelpAttempts++;
			  }
			  goto tryagain;
			}
		  }
		  else
		  {
			if (rval != cdesc->m_OldVal)
			{
			  newStatus = FAILED;
			}
		  }

		}
		// Advancing the MWCAS to the second phase succeeds only if it's still UNDECIDED.
		// This is the commit point of the operation because the second phase cannot fail.
		UINT32 oldState = InterlockedCompareExchange((LONG*)(&m_DescStatus.Status32), newStatus, UNDECIDED);
	}

	// SECOND PHASE ------------------------------------------------

	// If the first phase succeeded, all target words contains a pointer to this descriptor.
	// CAS in the new values to all target words.
	// If the first phase failed, some target words contain a pointer to this descriptor
	// and the others contain something else, that is, not a pointer to this descriptor.
	// CAS:ing back in the old value will succeed only for those that contain a pointer 
	// to this descriptor.
	bool succeeded = (m_DescStatus.Status32 == SUCCEEDED);

	for (int i = 0; i < m_Count; i++)
	{
	  CondCASDescriptor* cdesc = &m_CondCASDesc[i];
	  LONGLONG replVal = (succeeded) ? cdesc->m_NewVal : cdesc->m_OldVal;
			
	  LONGLONG curVal = 0;
	  do
	  {
		curVal = *cdesc->m_TargetAddr;
		if (IsCondCASDescriptor(curVal, m_FlagBitMask))
		{
		  CondCASDescriptor* desc = (CondCASDescriptor*)(ClearDescriptorFlag(curVal, m_FlagBitMask));
		  desc->CompleteCondCAS();
		}
	  } while (IsCondCASDescriptor(curVal, m_FlagBitMask));

	  LONGLONG rval = InterlockedCompareExchange64(cdesc->m_TargetAddr, replVal, descptr);
      cdesc->m_FinalVal = (rval == descptr)? replVal: rval;
	}


	// Update descriptor before exiting
	do
	{
	  curStatus = m_DescStatus;
	  newStatus = curStatus;
	  // Always decrement ref count, of course
	  newStatus.RefCount--;

	  // but only change descriptor state to FINISHED if about to release the last reference
	  if (curStatus.RefCount == 1)
	  {
		m_SavedOutcome = m_DescStatus.Status32;
		newStatus.Status32 = FINISHED;
		newStatus.RefCount += ~RefCountMask; // Set bit to disable helping
	  }

	} while (curStatus.Status64 !=
	  InterlockedCompareExchange64((LONGLONG*)&m_DescStatus.Status64, newStatus.Status64, curStatus.Status64));

	// When exiting this loop and curStatus.RefCount == 1, no other threads have a pointer to the descriptor
	// so we can return the finished descriptor to the partition's free list
	if (curStatus.RefCount == 1)
	{
	  assert(m_OwnerPartition);
	  assert(m_DescStatus.Status32 == FINISHED);
	  ReturnDescriptorToPool();
	}


	if (succeeded) stats->m_Succeded++; else stats->m_Failed++;

	return (succeeded);
}

bool MwCASDescriptor::VerifyUpdates()
{
    if (m_SavedOutcome == SUCCEEDED)
    {
        for (INT32 i = 0; i < m_Count; i++)
        {
            if (m_CondCASDesc[i].m_NewVal != m_CondCASDesc[i].m_FinalVal)
            {
                return false;
            }
        }
    }
    return true;
}


void MwCASDescriptor::PrintDescriptor()
{
  printf("Descriptor %I64X: %s(%d)\n", LONGLONG(this), (m_SavedOutcome == 2) ? "FAILED" : "SUCCEEDED", m_SavedOutcome);
  for (INT i = 0; i < m_Count; i++)
  {
	CondCASDescriptor* cdesc = &m_CondCASDesc[i];
	printf("Word %d(addr=%I64X, old=%I64X, new=%I64X, final=%I64X)\n", i, ULONGLONG(const_cast<LONGLONG*>(cdesc->m_TargetAddr)), 
	           cdesc->m_OldVal, cdesc->m_NewVal, cdesc->m_FinalVal);
  }

}


