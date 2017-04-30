//-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#pragma once

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#define LO_NIBBLE(b) ((BYTE)((BYTE)(b) & 0xF))
#define HI_NIBBLE(b) ((BYTE)(((BYTE)(b) >> 4) & 0xF))


/**
* Check if \a address is algined on \a alignment boundaries.
*
* \param address
*   Address to check
* \param alignment
*   Alignment to check for
*
* \returns
*   true of address is aligned on alignment boundary, false otherwise.
*/
inline bool IsAlignedOn(
  VOID const * const address,
  UINT_PTR alignment)
{
  return ((reinterpret_cast<UINT_PTR> (address)& (alignment - 1)) == 0);
}

/*
* The minimum alignment to guarantee atomic operations on a platform for both
* value types and pointer types
* The maximum size to guarantee atomic operations with the primitives below
*/
#if defined (_M_I386)
#define AtomicAlignment  4
#define PointerAlignment 4
#define AtomicMaxSize    4
#else
#define AtomicAlignment  4
#define PointerAlignment 8 
#define AtomicMaxSize    8 
#endif

/**
* A store with release semantics for a type T
*
* Notes:
*     1) Guarantees:
*        a) No Write reordering
*     b) Whole Write
*/
template <typename T> __forceinline void StoreWithRelease(
    __out T* destination,
    __in T value)
{
    static_assert(sizeof(T) <= AtomicMaxSize, "Type must be aligned to native pointer alignment.");
    //assert((((uintptr_t)destination) & (AtomicAlignment - 1)) == 0);

    // COMPILER-WISE:  
    // A volatile write is not compiler reorderable w/r to writes
    // PROCESSOR-WISE: 
    // X86 and X64 do not reorder stores. IA64 volatile emits st.rel,
    //  which ensures all stores complete before this one
    // 
    *((volatile T*)destination) = value;
}

/**
* A load with acquire semantics for a type T
*
* Notes:
*     1) Guarantees:
*        a) No read reordering
*        b) Whole Read
*        c) Immediate Read
*
* \return T
*/
template <typename T> __forceinline __checkReturn T LoadWithAcquire(
    __in T const * const source)
{
    static_assert(sizeof(T) <= AtomicMaxSize, "Type must be aligned to native pointer alignment.");
    assert((((uintptr_t)source) & (AtomicAlignment - 1)) == 0);

    // COMPILER-WISE:  
    // a volatile read is not compiler reorderable w/r to reads. However,
    //  we put in a _ReadBarrier() just to make sure
    // PROCESSOR-WISE: 
    // Common consensus is that X86 and X64 do NOT reorder loads. 
    // IA64 volatile emits ld.aq,which ensures all loads complete before this one
    //
    _ReadBarrier();
    return *((volatile T*)source);
}

/**
* An "immediate" load for a type T
*
* Notes:
*     1) Guarantees:
*        a) Whole Read
*        b) Immediate Read
*
* \return T
*/
template <typename T> __forceinline __checkReturn T LoadImmediate(
    __in T const * const source)
{
    static_assert(sizeof(T) <= AtomicMaxSize, "Type must be aligned to native pointer alignment.");
    assert((((uintptr_t)source) & (AtomicAlignment - 1)) == 0);

    // The volatile qualifier has the side effect of being a load-aquire on
    // IA64. That's a result of overloading the volatile keyword.
    //
    return *((volatile T*)source);
}


