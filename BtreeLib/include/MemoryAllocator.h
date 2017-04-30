#pragma once

#include <Windows.h>


// Interface that a custom memory allocator needs to implement.
class IMemoryAllocator
{
public:
  virtual HRESULT Allocate(__in DWORD nBytes, __out void** ppBytes) = 0;
  virtual HRESULT Free(__in void* pBytes) = 0;
  virtual HRESULT GetAllocatedSize(__in void* pBytes, __out DWORD* pnAllocatedSizee) = 0;

  virtual HRESULT AllocateAligned(__in DWORD nBytes, __in DWORD nAlignment, __out void** ppBytes) = 0;
  virtual HRESULT FreeAligned(__in void* pBytes, __in DWORD nAlignment) = 0;
  virtual HRESULT GetAlignedAllocatedSize(__in void* pBytes, __out DWORD* pnAllocatedSizee) = 0;

};