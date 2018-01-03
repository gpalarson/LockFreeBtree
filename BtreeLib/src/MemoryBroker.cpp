/* =========================================================================================
* The files MemoryBroker.h and MemoryBroker.cpp contain an implementation of a broker or clerk
* that tracks the amount of memory allocated in total and by object type. It simply intercepts
* calls to the memory allocator and updates its counters based on the type specified in the call.
*
* TODO: The implementation is mostly generic but a few traces of its use for a hash table
* should be removed.
*
* Paul Larson, gpalarson@outlook.com, Dec 2016
==========================================================================================*/

#include <Windows.h>
#include <crtdbg.h>
#include <stdio.h>
#include "MemoryBroker.h"
#include "BtreeInternal.h"

// The memory broker uses this allocator if no other is specified
static DefaultMemoryAllocator s_defaultMemoryAllocator;

IMemoryAllocator* GetDefaultMemoryAllocator()
{
  return static_cast<IMemoryAllocator*>(&s_defaultMemoryAllocator);
}


// Constructor
MemoryBroker::MemoryBroker(IMemoryAllocator* memAllocator)
  : m_nMemoryAllocatedCount(0)
{
  m_pMemoryAllocator = (memAllocator) ? memAllocator : &s_defaultMemoryAllocator;
  for (int i = 0; i < s_TypeCount; i++) m_nAllocatedBytes[i] = 0;
}

// Returns the total memory used, memory used by each object type, and 
// memory currently on the GC deallocation lists
//
__checkReturn HRESULT MemoryBroker::GetTotalMemoryUsage(
  __out __int64* pnMemoryUsageTotal, 
  __out __int64* pnMemoryUsageIndexPages,
  __out __int64* pnMemoryUsageLeafPages, 
  __out __int64* pnMemoryUsageInGC,
  __out __int64* pnMemoryUsagePtrArray )
{
  if (!pnMemoryUsageTotal) return E_POINTER;
  *pnMemoryUsageTotal       = m_nMemoryAllocatedCount;
  *pnMemoryUsageIndexPages  = m_nAllocatedBytes[(int)MemObjectType::IndexPage];
  *pnMemoryUsageLeafPages   = m_nAllocatedBytes[(int)MemObjectType::LeafPage];
  *pnMemoryUsageInGC        = m_nAllocatedBytes[(int)MemObjectType::GCItemObj];
  *pnMemoryUsagePtrArray    = m_nAllocatedBytes[(int)MemObjectType::TmpPointerArray];
  return S_OK;
}



// Increment the allocate bytes counter for objects of the given type
// Returns S_OK if type is known, E_UNEXPECTED if type is unknown.
//
__checkReturn HRESULT MemoryBroker::IncrementAllocationCounters(
  __in MemObjectType type,
  __in size_t size)
{
  if (type >= MemObjectType::First && type <= MemObjectType::Last)
  {
	::InterlockedExchangeAdd64(&m_nAllocatedBytes[(int)type], (__int64)(size));
  }
  else {
	ASSERT_WITH_TRACE(false, "unknown allocation type");
	return E_UNEXPECTED;
  }

  return S_OK;
}

// Decrement the allocated bytes counter for the given type.
// Returns S_OK if type is known, E_UNEXPECTED if type is unknown.
//
__checkReturn HRESULT MemoryBroker::DecrementAllocationCounters(
  __in MemObjectType type,
  __in size_t size)
{
  if (type >= MemObjectType::First && type <= MemObjectType::Last)
  {
	::InterlockedExchangeAdd64(&m_nAllocatedBytes[(int)type], -((__int64)(size)));
  }
  else {
	ASSERT_WITH_TRACE(false, "unknown allocation type");
	return E_UNEXPECTED;
  }

  if (m_nAllocatedBytes[(int)type] < 0)
  {
	ASSERT_WITH_TRACE(false, "negative memory allocation counter");
	return E_UNEXPECTED;
  }

  return S_OK;
}

// Allocate a block of memory and update memory usage counters.
// Returns S_OK if allocation succeeded, E_POINTER when ppvMemory is NULL,
// Can also return errors from the call to the memory allocator.
//
__checkReturn HRESULT MemoryBroker::Allocate(
  __in size_t  nSize,
  __out void** ppvMemory,
  __in_opt MemObjectType type)
{
  if (!ppvMemory) return E_POINTER;

  HRESULT hr = m_pMemoryAllocator->Allocate(static_cast<DWORD>(nSize), ppvMemory);
  if (FAILED(hr)) return hr;

  ULONG allocatedSize = 0;
  m_pMemoryAllocator->GetAllocatedSize(*ppvMemory, &allocatedSize);

  ::InterlockedExchangeAdd64(&m_nMemoryAllocatedCount, allocatedSize);

  hr = IncrementAllocationCounters(type, allocatedSize);
  CHECK_HRESULT(hr);

  return hr;
}

__checkReturn HRESULT MemoryBroker::AllocateAligned(__in DWORD nSize, __in DWORD nAlignment, __out void** ppBytes, __in_opt MemObjectType type)
{
  if (!ppBytes) return E_POINTER;

  HRESULT hr = m_pMemoryAllocator->AllocateAligned(static_cast<DWORD>(nSize), nAlignment, ppBytes);
  if (FAILED(hr)) return hr;

  ULONG nAllocatedSize = 0;
  hr = m_pMemoryAllocator->GetAlignedAllocatedSize(*ppBytes, &nAllocatedSize);
  ::InterlockedExchangeAdd64(&m_nMemoryAllocatedCount, nAllocatedSize);

  hr = IncrementAllocationCounters(type, nAllocatedSize);
  CHECK_HRESULT(hr);

  return hr;
}

// Free a block of memory and update memory usage counters.
// Returns S_OK if block freed successfully, E_POINTER when pBytes is NULL,
// and E_UNEXPECTED if counter goes negative
// Can also return errors from the call to the memory allocator.
//
__checkReturn HRESULT MemoryBroker::Free(__in void* pBytes, __in MemObjectType type)
{
  if (!pBytes) return E_POINTER;

  ULONG nAllocatedSize = 0;
  HRESULT hr = m_pMemoryAllocator->GetAllocatedSize(pBytes, &nAllocatedSize);
  
#ifndef DO_LOG
  hr = m_pMemoryAllocator->Free(pBytes);
  if (FAILED(hr)) return hr;
#endif
 
  ::InterlockedExchangeAdd64(&m_nMemoryAllocatedCount, -(__int64)(nAllocatedSize));

  if (m_nMemoryAllocatedCount < 0)
  {
	ASSERT_WITH_TRACE(false, "allocation count < 0");
	return E_UNEXPECTED;
  }

  hr = DecrementAllocationCounters(type, static_cast<size_t>(nAllocatedSize));
  CHECK_HRESULT(hr);

  return hr;
}

__checkReturn HRESULT MemoryBroker::GetAllocatedSize(__in void* pBytes, __out DWORD* pnAllocatedSizee)
{
  return m_pMemoryAllocator->GetAllocatedSize(pBytes, pnAllocatedSizee);
}

__checkReturn HRESULT MemoryBroker::FreeAligned(__in void* pBytes, __in DWORD nAlignment, __in MemObjectType type)
{
  if (!pBytes) return E_POINTER;

  ULONG nAllocatedSize = 0;
  HRESULT hr = m_pMemoryAllocator->GetAlignedAllocatedSize(pBytes, &nAllocatedSize);
 
#ifdef DO_LOG
  // Don't free memory if we are logging actions 
  hr = m_pMemoryAllocator->FreeAligned(pBytes, nAlignment);
  if (FAILED(hr)) return hr;
#endif

  ::InterlockedExchangeAdd64(&m_nMemoryAllocatedCount, -(__int64)(nAllocatedSize));

  if (m_nMemoryAllocatedCount < 0 )
  {
	ASSERT_WITH_TRACE(false, "allocation count < 0");
	return E_UNEXPECTED;
  }

  hr = DecrementAllocationCounters(type, static_cast<size_t>(nAllocatedSize));
  CHECK_HRESULT(hr);

 
  return hr;
}

__checkReturn HRESULT MemoryBroker::GetAlignedAllocatedSize(__in void* pBytes, __out DWORD* pnAllocatedSizee)
{
  return m_pMemoryAllocator->GetAlignedAllocatedSize(pBytes, pnAllocatedSizee);
}


// Deallocate a memory block immediately (without epoch protection). 
// Return values
// S_OK  Deallocation finished successfully.
// E_POINTER Null input argument.
// E_UNEXPECTED Unexpected state - no reference to a memory allocator.
// Function can also return error from call to memory allocator.
//
__checkReturn HRESULT MemoryBroker::DeallocateNow(__in void* pvMemoryToFree,  __in MemObjectType type)
{
  if (!pvMemoryToFree) return E_POINTER;

  if (!m_pMemoryAllocator)
  {
	ASSERT_WITH_TRACE(false, "null memory allocator");
	return E_UNEXPECTED;
  }

  DWORD nAllocatedSize = 0;
  HRESULT hr = m_pMemoryAllocator->GetAllocatedSize(pvMemoryToFree, &nAllocatedSize);
  if (FAILED(hr))
  {
	ASSERT_WITH_TRACE(false, "GetallocatedSize failed");
	return E_UNEXPECTED;
  }

  hr = m_pMemoryAllocator->Free(pvMemoryToFree);
  if (FAILED(hr)) return hr;

  ::InterlockedExchangeAdd64(&m_nMemoryAllocatedCount, -((__int64)nAllocatedSize));
  if (m_nMemoryAllocatedCount < 0)
  {
	ASSERT_WITH_TRACE(false, "allocation count < 0");
	return E_UNEXPECTED;
  }

  hr = DecrementAllocationCounters(type, static_cast<size_t>(nAllocatedSize));
  CHECK_HRESULT(hr);

  return hr;
}



