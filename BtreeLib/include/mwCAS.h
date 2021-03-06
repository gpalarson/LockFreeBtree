
// ***************************************************************************
// The files mwCAS.h and mwCAS.cpp contain an implementation of a 
// multi-word compare-and-swap (MWCAS) operation. The operation is 
// lock free and non-blocking. It requires a flag bit in each word
// that participates in an MWCAS operation (called an MwCasTargetField). 
// It is primarily designed for atomically swapping 64-bit pointers  
// but can be used for any data type of size 63 bits or less.
// 
// The implementation uses a preallocated, partitoned pool of MWCAS
// descriptors and relies on reference counting to protect access
// to descriptors. 
// 
// To perform an MWCAS operation, the user first allocates a descriptor and
// initializes it with target addresses, old values, and new values and then 
// calls mwCAS() on the descriptor. The return value indicates whether the 
// operations succeded or not.
// 
// The implementation is based on the algorithms in the paper:
//    Harris, Timothy L., Fraser, Keir, Pratt, Ian A., 
//    A Practical Multi-word Compare-and-Swap Operation", DISC 2002, 265-279
//
//  Paul Larson, August 2016, gpalarson@outlook.com
// ***************************************************************************

#pragma once

#include <Windows.h>
#include <typeinfo.h>
#include <stdio.h>
#include <assert.h>
#include <crtdbg.h>

// Forward references
class MwCasDescriptorPartition;
class MwCasDescriptorPool;
class MwCASDescriptor;
class CondCASDescriptor;
class BtreePage;

// Static parameters
static const UINT MaxWordsPerDescriptor = 6;
static const UINT DefaultPoolPartitions = 4;
static const UINT DefaultDescPerPartition = 8;

static const UINT CACHE_LINE_SIZE = 64;


class MwCasDescriptorBase
{
 
protected: 
   enum DescriptorType:INT32 { CONDCAS_DESCRIPTOR=0, MWCAS_DESCRIPTOR }; 

  // A flag bit indicates whether a pointer is "clean" or points to a descriptor.
  // The descriptor's Type field indicates the descriptor type.	
  DescriptorType			m_Type;	

public:
  
  MwCasDescriptorBase(DescriptorType type)
  {
	m_Type = type;
  }
  
  static LONGLONG SetDescriptorFlag(MwCasDescriptorBase* ptr, UINT64 mask)
  {
	return LONGLONG(UINT64(ptr) | mask);
  }

  static MwCasDescriptorBase* ClearDescriptorFlag(LONGLONG ptr, UINT64 mask)
  {
	return (MwCasDescriptorBase*)(INT64(ptr) & ~mask);
  }


  static bool IsDescriptorPtr(LONGLONG ptr, UINT64 mask)
  {
	return (INT64(ptr) & mask) != 0 ;
  }

  static bool IsCondCASDescriptor(LONGLONG ptr, UINT64 mask)
  {
	return ptr != 0 && IsDescriptorPtr(ptr, mask) && (ClearDescriptorFlag(ptr, mask)->m_Type == CONDCAS_DESCRIPTOR);
  }

  static bool IsMwCASDescriptor(LONGLONG ptr, UINT64 mask)
  {
	return ptr != 0 && IsDescriptorPtr(ptr, mask) && (ClearDescriptorFlag(ptr, mask)->m_Type == MWCAS_DESCRIPTOR);
  }
};


// A CondCASDesriptor is used for implementing a conditional CAS operation, CondCAS,
// where the target variable is updated to NewVal only if its value matches OldVal
// and, in addition, a (condition) variable has the value UNDECIDED
// CondCAS is used in the first phase of the MwCAS operation to atomically set each target
// variable to point to its MwCASDescriptor but only if the MwCASDescriptor status is still UNDECIDED.
//
class  CondCASDescriptor : public MwCasDescriptorBase
{
  friend class MwCASDescriptor;

  MwCASDescriptor*	m_OwnerDesc;		// Backpointer to parent descriptor

  LONGLONG*			m_TargetAddr;		// Address of word to be updated
  LONGLONG			m_OldVal;			// Expected old value
  LONGLONG			m_NewVal;	        // New value to assign to target after a (successful) MwCAS operation
  LONGLONG          m_FinalVal;         // Actual final value of the target workd
  
public:

  CondCASDescriptor()
	:MwCasDescriptorBase(CONDCAS_DESCRIPTOR)
  {
	m_TargetAddr = nullptr;
	m_OldVal = m_NewVal = m_FinalVal = 0;
  }

  // Execute the conditional CAS operation
  LONGLONG CondCAS(LONGLONG& finalVal, bool& ignoreResult);
 
 // Swap in the new value if the condition variable has the expected value,
  // otherwise restore the old value.
  LONGLONG CompleteCondCAS( bool hasAccess = false);
 
  static LONGLONG CondCASRead(LONGLONG* addr, UINT64 mask);
   
};


class alignas(CACHE_LINE_SIZE) MwCASDescriptor : public MwCasDescriptorBase
{

	friend class MwCasDescriptorPool;
	friend class MwCasDescriptorPartition;
	friend class CondCASDescriptor;

	MwCASDescriptor*			m_NextDesc;

	// Backpointer to owning partition so the descriptor can be
	// returned to its home partition when its freed.
	MwCasDescriptorPartition*	m_OwnerPartition;

	// 64-bit mask for extracting, setting, and clearing the bit
	// that indicates whether a word contains a descriptor pointer or not.
	// The same bit is used in every target word.
	UINT64						m_FlagBitMask;
		
	// Descriptor states. Valid transitions are as follows:
	enum eDescState:INT32 {FILLED, UNDECIDED, FAILED, SUCCEEDED, FINISHED } ;

	// The high order bit of RefCount is used as a flag to indicate
	// whether helping is allowed (bit is 0) or not (bit is 1).
	// The flag is set as soon as the MwCas opeartion has finished.
	// This mask is used to extract the actual refcount (without the bit).
	static const UINT32 RefCountMask = 0x7fffffff;


	// Tracks the current status of the descriptor. 
	// State and ref count packed into 64-bit word so they can be 
	// updated atomically together. 
	union uStatus
	{
		struct
		{
			volatile eDescState		Status32;
			volatile UINT32			RefCount;
		};
		volatile LONGLONG Status64;
	} m_DescStatus ;

	// Array of word descriptors
	INT32				m_Count ;
	CondCASDescriptor   m_CondCASDesc[MaxWordsPerDescriptor];

	// Fields used for debugging only
	eDescState			m_SavedOutcome;

	void ReturnDescriptorToPool();

	static bool IsHelpingAllowed(UINT32 refcount)
	{
	  return (refcount & ~RefCountMask) == 0;
	}

public:

	MwCASDescriptor(MwCasDescriptorPartition* owner);
	
	// Adds information about a new word to be modifiec by the MwCAS operator.
	// Word descriptors are stored sorted on the word address to prevent livelocks.
	// Return value is negative if the descriptor is full.
	INT32 AddEntryToDescriptor( __in LONGLONG* addr, __in LONGLONG oldval, __in LONGLONG newval) ;

	// Closes a descriptor after it has been filled with the required info.
	// After that it's ready to be executed.
	void CloseDescriptor()
	{
		uStatus st = m_DescStatus;
		assert(st.Status32 == FINISHED);
		assert(st.RefCount == ~RefCountMask);
		m_DescStatus.Status32 = FILLED ;
	}

	// Execute the multi-word compare and swap operation.
	bool MwCAS(UINT calldepth=0);

	// Called before helping to complete an MwCAS operation.
	// Returns true if helping is allowed and also increases the descriptor's refcount. 
	// Returns false if helping is no longer allowed.
	bool SecureAcces();

	// Decrements the ref count. 
	// Return descriptor to the pool if ref count goes to zero.
	void ReleaseAccess();

    // Fields that may be the target of MwCAS operations must be read
    // using this function. It checks whether the field contains a pointer
    // to a descriptor and, if so, helps complete the ongoing MwCAS operations.
    // It always returns a "clean" value, never a pointer to a descriptor.
	static LONGLONG MwCASRead(LONGLONG* addr, UINT64 typeMask)
	{
	  LONGLONG rval = 0;
	  bool isMwCasDesc = false;
      // TODO: temporary kludge to see hom much it improves performance - fix later
      ULONGLONG ptrMask = ULONGLONG(1) << 63;

      // Try fast path first
      rval = *addr;
      if (!IsDescriptorPtr(rval, ptrMask))
      {
          return rval;
      }

      // Descriptor flag is set so use slow(er) path
	  do
	  {
		rval = CondCASDescriptor::CondCASRead(addr, typeMask);
		isMwCasDesc = MwCasDescriptorBase::IsMwCASDescriptor(LONGLONG(rval), typeMask);
		if (isMwCasDesc)
		{
		  ((MwCASDescriptor*)MwCasDescriptorBase::ClearDescriptorFlag(rval, typeMask))->MwCAS(1);
		}

	  } while (isMwCasDesc );

	  _ASSERTE(!isMwCasDesc);
	  _ASSERTE(!IsDescriptorPtr(rval, typeMask));
	  return rval ;
	}

    bool VerifyUpdates();

	void PrintDescriptor();

} ;

//static_assert (sizeof(MwCASDescriptor) <= 5*64, "MwCASDescriptor larger than five cache lines");

// Struct for counting the outcome of MwCAS operations
struct MwCasCounts
{
    ULONG			m_Attempts;         // No of MwCAS calls executed
    ULONG			m_Bailed;           // Bailed out because operation had been completed by another thread
    ULONG			m_Succeded;         // No of successful calls
    ULONG			m_Failed;           // No of failed calls
    ULONG           m_HelpAttempts;     // No of attempts to help

    void InitCounts()
    {
        m_Attempts = m_Bailed = m_Succeded = m_Failed = m_HelpAttempts = 0;
    }

    MwCasCounts()
    {
        InitCounts();
    }
};

static const UINT32 s_MaxStatsDepth = 5;

// Counts are kept for different call depths. Level 0 is original call,
// level 1 is an attempt to help an origional call, level 2 is an attempt
// to help a call at level 1, and so on
struct MwCasStats
{
    MwCasCounts     Counts[s_MaxStatsDepth];

    void InitStats()
    {
        for (UINT32 i = 0; i < s_MaxStatsDepth; i++) Counts[i].InitCounts();
    }

    // Add up the counts from different partitions
    void AddCounts(MwCasCounts* partCounts);
    void PrintStats();
};


// Partitioned pool of MwCASDescriptor and CondCASDescriptor objects.
// If implementing persistent MwCAS then MwCASDescriptors must be stored
// in NVRAM. 
class alignas(CACHE_LINE_SIZE) MwCasDescriptorPartition
{

public:
  MwCasCounts					m_StatsCounts[s_MaxStatsDepth];

private:
  volatile MwCASDescriptor*		m_MwDescrFreeList;	  // Free list
  MwCasDescriptorPool*			m_MwDescrPool;		  // Back pointer to the owner pool
  INT32							m_DescCount;		  // Nr of descriptors in the partition


  // Push a descriptor onto the free list
  void PushOntoQueue(__in MwCASDescriptor* pNewFirst)
  {
	_ASSERTE(((UINT64(pNewFirst) % MEMORY_ALLOCATION_ALIGNMENT) == 0));

	if (pNewFirst)
	{
	  LONG64 resVal = 0;
	  MwCASDescriptor* pFirst = nullptr;
	  do
	  {
		pFirst = const_cast<MwCASDescriptor*>(m_MwDescrFreeList);
		pNewFirst->m_NextDesc = pFirst;
		resVal = InterlockedCompareExchange64((LONG64*)(&m_MwDescrFreeList), LONG64(pNewFirst), LONG64(pFirst));
	  } while (resVal != LONG64(pFirst));

	}
  }

  // Try to pop a descriptor from the free list
  MwCASDescriptor* PopFromQueue()
  {
	MwCASDescriptor* pFirst = nullptr;
	MwCASDescriptor* pNext = nullptr;
	LONG64 resVal = 0;
	do
	{
	  pFirst = const_cast<MwCASDescriptor*>(m_MwDescrFreeList);
	  if (pFirst == nullptr) break;

	  pNext = pFirst->m_NextDesc;
	  resVal = InterlockedCompareExchange64((LONG64*)(&m_MwDescrFreeList), LONG64(pNext), LONG64(pFirst));
	} while (resVal != LONG64(pFirst));

	return pFirst;
  }


public:

  MwCasDescriptorPartition(MwCasDescriptorPool* ownerPool, UINT preallocate);
  ~MwCasDescriptorPartition();
 
  MwCASDescriptor* AllocateMwCASDescriptor()
  {
	return PopFromQueue();
  }

  void ReleaseMwCASDescriptor(MwCASDescriptor* desc)
  {
	_ASSERTE(desc);

	if (desc)
	{
	  _ASSERTE(desc->m_DescStatus.RefCount == ~MwCASDescriptor::RefCountMask + 0);
	  _ASSERTE(desc->m_DescStatus.Status32 == MwCASDescriptor::FINISHED);
	  PushOntoQueue(desc);
	}
  }
 
 };

// Finally here is the definition of the descriptor pool.
// The single, global instance of the pool is declared in MwCAS.cpp
//
class MwCasDescriptorPool
{
	friend MwCasDescriptorPartition; 

    // Total number of descriptors in the pool
	UINT32			  m_MaxPoolSize;
	UINT32            m_DescInPool;
	
	// Spread access to the pool by hashing on thread ID
	UINT32						m_PartitionCount;
	MwCasDescriptorPartition*	m_PartitionTbl;

    // Need to scramble the thread ID to get a good distribution
	UINT HashThreadId(UINT id)
	{
	  const UINT32 Const = 0X12B9B6A5;

	  UINT64 hashVal = id*Const;
	  hashVal = hashVal >> 5;
	  return UINT(hashVal & 0xffffffff);
	}
	
public:
	MwCasDescriptorPool(UINT partitions= DefaultPoolPartitions, UINT preallocate=DefaultDescPerPartition);
	~MwCasDescriptorPool();

	// Get a free MwCASDescriptor from the pool.
	MwCASDescriptor* AllocateMwCASDescriptor(ULONG mask);

	// Gather and print stats about MwCasOperations
	void PrintMwCasStats();
};

MwCASDescriptor* AllocateMwCASDescriptor(ULONG flagPos);

template < class T, int FlagPos = 0>
class MwcTargetField
{
  static_assert(sizeof(T) == 8, "MwCTargetField type is not of size 8 bytes");
  static_assert(FlagPos >= 0 && FlagPos < 64, "MwcTargetField flag position out of range");

  volatile T		m_Value;
    
public:
  static const UINT64 typeMask = UINT64(1) << FlagPos;

  MwcTargetField(void* desc = nullptr)
  {
	m_Value = T(desc);
  }

  T Read()
  {
	return T(MwCASDescriptor::MwCASRead((LONGLONG*)(&m_Value), typeMask));
  }

  LONGLONG ReadLL()
  {
	return LONGLONG(MwCASDescriptor::MwCASRead((LONGLONG*)(&m_Value), typeMask));
  }

  T ReadPP()
  {
	return (T)(MwCASDescriptor::MwCASRead((LONGLONG*)(&m_Value), typeMask));
  }


  operator LONGLONG()
  {
	return LONGLONG(m_Value);
  }

  // Copy operator
  MwcTargetField<T, FlagPos>& operator= (MwcTargetField<T, FlagPos>& rhval)
  {
	m_Value = rhval.m_Value;
	return *this;
  }

  // Address-of operator
  T* operator& ()
  {
	return const_cast<T*>(&m_Value);
  }


  // Assignment operator
  MwcTargetField<T, FlagPos>& operator= (T rhval)
  {
	m_Value = rhval;
	return *this;
  }


  // Content-of operator
  T& operator* ()
  {
	return *m_Value;
  }

  // Dereference operator
  T* operator-> ()
  {
	return m_Value;
  }

};


