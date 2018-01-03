/* =========================================================================================
* The files MemoryBroker.h and MemoryBroker.cpp contain an implementation of a broker or clerk
* that tracks the amount of memory allocated in total and by object type. It simply intercepts
* calls the memory allocator and updates its counters based on the type of call.
*
* TODO: The implementation is mostly generic but a few traces of its use for a hash table 
* should be removed.
*
* Paul Larson, gpalarson@outlook.com, Dec 2016
==========================================================================================*/

#pragma once

#include <windows.h>
#include <crtdbg.h>
#include "MemoryAllocator.h"


#define ASSERT_WITH_TRACE(condition, msg, ...) _ASSERTE(( L ## #condition ## msg, condition));


#ifdef _DEBUG
#define CHECK_HRESULT(expression) \
{ { \
    HRESULT ___hr = expression; \
    if(FAILED(___hr)) { \
    fwprintf(stderr, L"FAILED at %s:%s:%d, hr=0x%x\n", __FUNCTIONW__, __FILEW__, __LINE__, ___hr); \
    return ___hr; \
    } \
} }


#else
#define CHECK_HRESULT(expression) \
{ { \
    HRESULT ___hr = expression; \
    if(FAILED(___hr)) return ___hr; \
} }

#endif

// The memory broker tracks memory usage by object type.
// This enum lists the different object types used by the hash table 
// and its epoch manager. The function provides the name of a given type.
enum MemObjectType : int
{
  Invalid = -1,

  IndexPage,
  LeafPage,
  TmpPointerArray,
  GCItemObj,

  First = IndexPage,
  Last = GCItemObj
};

static const int s_TypeCount = (int)MemObjectType::Last - (int)MemObjectType::First + 1;

// Provides the name of the given object type.
//
static char* NameOfMemObjectType(MemObjectType type)
{
  static char Name[s_TypeCount][10] = { "IndexPage", "LeafPage", "PtrArray", "GCItem" };

  char* str = "InvalidType";
  if (type >= MemObjectType::First && type <= MemObjectType::Last)
  {
	str = &Name[(int)type][0];
  }
  return str;
}

// The code from here on is generic and not specific to the hash table implementation
// ==================================================================================

class IMemoryAllocator;

// Tracks the amount of memory allocated to objects of different type.
//
class MemoryBroker 
{
  volatile __int64 m_nMemoryAllocatedCount;		  // Total allocated memory (in bytes).
  volatile __int64 m_nAllocatedBytes[s_TypeCount];  // Memory allocated by type (in bytes)

  IMemoryAllocator* m_pMemoryAllocator;			  // The actual memory allocator used.

public:
  MemoryBroker(IMemoryAllocator* memAllocator = nullptr);
  ~MemoryBroker()
  {
	_ASSERTE(m_nMemoryAllocatedCount == 0);
  }

  // Functions implementing the IMemoryAllocator interface
  __checkReturn HRESULT Allocate(__in size_t nSize, __out void** ppvMemory, __in_opt MemObjectType type);
  __checkReturn HRESULT Free(__in void* pBytes, __in MemObjectType type);
  __checkReturn HRESULT GetAllocatedSize(__in void* pBytes, __out DWORD* pnAllocatedSizee);
    
  __checkReturn HRESULT AllocateAligned(__in DWORD nBytes, __in DWORD nAlignment, __out void** ppBytes, __in_opt MemObjectType type);
  __checkReturn HRESULT FreeAligned(__in void* pBytes, __in DWORD nAlignment, __in MemObjectType type);
  __checkReturn HRESULT GetAlignedAllocatedSize(__in void* pBytes, __out DWORD* pnAllocatedSizee);

  // Immediate deallocation
  __checkReturn HRESULT DeallocateNow(__in void* pvMemoryToFree, __in MemObjectType type);
 
  // TODO: This is specific to hash table implementation - make it generic.
  __checkReturn HRESULT GetTotalMemoryUsage(
	__out __int64* pnMemoryUsageTotal,
	__out __int64* pnMemoryUsageIndexPages,
	__out __int64* pnMemoryUsageLeafPages,
	__out __int64* pnMemoryUsageInGC,
	__out __int64* pnMemoryUsagePtrArray);
 
private:
  // Update allocation counters
  __checkReturn HRESULT IncrementAllocationCounters(__in MemObjectType type, __in size_t size);
  __checkReturn HRESULT DecrementAllocationCounters(__in MemObjectType type,	__in size_t size);

};

// Allocator used when the user does not specify a memory allocator.
// It simply calls malloc and free.
class DefaultMemoryAllocator : public IMemoryAllocator
{
public:
  HRESULT Allocate(
	__in DWORD nBytes,
	__out void** ppBytes)
  {
	HRESULT hr = S_OK;
	*ppBytes = (void*)::malloc(nBytes);
	if (*ppBytes == nullptr) hr = ERROR_OUTOFMEMORY;

	return hr;
  }

  HRESULT AllocateAligned( __in DWORD nBytes, __in DWORD nAlignment, __out void** ppBytes)
  {
	HRESULT hr = S_OK;
	*ppBytes = (void*)::_aligned_malloc(nBytes, nAlignment);
	if (*ppBytes == nullptr) hr = ERROR_OUTOFMEMORY;

	return hr;
  }

  HRESULT Free(__in void* pBytes)
  {
	::free(pBytes);
	return S_OK;
  }

  HRESULT FreeAligned(__in void* pBytes, __in DWORD nAlignment)
  {
	::_aligned_free(pBytes);
	return S_OK;
  }

 
  HRESULT GetAllocatedSize(	__in void* pBytes, 	__out DWORD* pnAllocatedSizee)
  {
	HRESULT hr = S_OK;
	if (pBytes)
	{
	  *pnAllocatedSizee = DWORD(::_msize(pBytes));
	  if (*pnAllocatedSizee == -1) hr = ERROR_INVALID_PARAMETER;
	}
	return hr;
  }

  HRESULT GetAlignedAllocatedSize(__in void* pBytes, __out DWORD* pnAllocatedSizee)
  {
	HRESULT hr = S_OK;
	if (pBytes)
	{
	  *pnAllocatedSizee = DWORD(::_aligned_msize(pBytes, MEMORY_ALLOCATION_ALIGNMENT,0));
	  if (*pnAllocatedSizee == -1) hr = ERROR_INVALID_PARAMETER;
	}
	return hr;
  }


 };

