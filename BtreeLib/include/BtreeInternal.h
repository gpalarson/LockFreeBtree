#pragma once
#include <windows.h>
#include "mwCAS.h"
#include "Utilities.h"
#include "MemoryBroker.h"
#include "EpochManager.h"
#include "MemoryBroker.h"

// Forward references
class BtreeRootInternal;
class BtLeafPage;
class BtIterator;

enum BTRESULT { BT_SUCCESS, BT_INVALID_ARG, BT_DUPLICATE_KEY, BT_KEY_NOT_FOUND, BT_OUT_OF_MEMORY, BT_INTERNAL_ERROR,
                BT_PAGE_CLOSED, BT_PAGE_FULL, BT_NOT_INSERTED, BT_MAX_SIZE, BT_MIN_SIZE, BT_INSTALL_FAILED};

// Defaul functions used unless user specifies otherwise
IMemoryAllocator* GetDefaultMemoryAllocator();
int   DefaultCompareKeys(const void* key1, const int keylen1, const void* key2, const int keylen2);

using CompareFn = int (const void* key1, const int keylen1, const void* key2, const int keylen2);

struct KeyType
{
    union
    {
        void*   m_pKeyValue;        // Pointer to key value
        UINT    m_oKeyValue;        // Offset to key value
    };
    UINT        m_KeyLen;           // Length in bytes

    KeyType(void* key=nullptr, UINT keyLen=0)
    {
        m_pKeyValue = key;
        m_KeyLen = keyLen;
    }

    KeyType(UINT keyOffset, UINT keyLen)
    {
        m_oKeyValue = keyOffset;
        m_KeyLen = keyLen;
    }

    void Get(void*& key, UINT& keyLen)
    {
        key = m_pKeyValue;
        keyLen = m_KeyLen;
    }

    static const char MinChar = 0;
    static const char MaxChar = -1; // sets it to 0xff
   
    static void GetMinValue(void*& value, UINT& len)
    {
        value = (void*)(&MinChar);
        len = 1;
    }

    static void GetMaxValue(void*& value, UINT& len)
    {
        value = (void*)(&MaxChar);
        len = 1;
    }
 };


// A key-pointer entry on an index page or a leaf page.
// On an index page, the pointer points to an index page or a leaf page.
// On a leaf apge, it points to a B-tree record.
#pragma pack(push)
#pragma pack(2)

struct KeyPtrPair
{
  UINT16            m_KeyOffset;	      // Offset of the key from the beginning of the page
  UINT16            m_KeyLen;             // Key length in bytes.
  volatile void*    m_RecPtr;             // The associated pointer

  static const ULONGLONG CloseBitMask = ULONGLONG(0x1)<<63;
                                 

  static bool IsClosedBitOn(ULONGLONG val)
  {
      return (val & CloseBitMask) != 0;
  }

  void Set(UINT16 keyoffset, UINT16 keylen, void* recptr)
  {
	m_KeyOffset = keyoffset;
	m_KeyLen = keylen;
	m_RecPtr = recptr;
  }

  void CloseEntry()
  {
      ULONGLONG oldVal = ULONGLONG(const_cast<void*>(m_RecPtr));
      ULONGLONG newVal = oldVal | CloseBitMask;
      if ((oldVal & CloseBitMask) == 0)
      {
          InterlockedCompareExchange64((LONGLONG*)(&m_RecPtr), newVal, oldVal);
      }
  }

  void CleanEntry()
  {
     ULONGLONG oldVal = ULONGLONG(const_cast<void*>(m_RecPtr));
     ULONGLONG newVal = oldVal & ~CloseBitMask;
     
      m_RecPtr = (void*)(newVal);
  }

  bool IsClosed()
  {
      return (ULONGLONG(const_cast<void*>(m_RecPtr)) & CloseBitMask) != 0;
  }
};

#pragma pack(pop)

enum ePageState : UINT16 { PAGE_NORMAL, PAGE_CLOSING, PAGE_CLOSED, SPLIT_PAGE, MERGE_PAGE};

struct PageStatus
{
  ePageState	  m_PageState;			  // Page status
  UINT16		  m_nUnsortedReserved;	  // Number of record slots that have been reserved
										  // in the unsorted area
  UINT16		  m_FirstFreeByte;		  // Offset of the first byte in the area for unsorted records
  UINT16		  m_SlotsCleared;		  // Slots that have been cleared because its record was deleted.
										  // Note that a reserved slot may be empty. A slot is empty if its record pointer field is null.
										  // m_SlotsReserved - m_SlotsCleared is a safe upper bound on the number of actaul records on the page
};

union PageStatusUnion
{
  volatile PageStatus	  m_Status;
  volatile LONG64		  m_Status64;
};

// A permutation array is a mapping that specifies the sorted ordering of an array of RecordEntries
struct PermutationArray
{
  BtLeafPage*             m_TargetPage;      // back pointer to the leaf page
  PageStatusUnion		  m_CreateStatus;	 // page status when the permutation array was created
  UINT					  m_nrEntries;       // Nr of entries in the array
  UINT16				  m_PermArray[1];	 // The actual permutation array begins here

  PermutationArray(BtLeafPage* page, UINT count, PageStatusUnion status)
  {
      m_TargetPage = page;
      m_CreateStatus.m_Status64 = status.m_Status64;
      m_nrEntries = count;
      for (UINT i = 0; i < count; i++) m_PermArray[i] = i;
  }

  BTRESULT SortPermArray();
 
};

class BtBasePage
{
  friend class BtreeRootInternal;

protected:
  enum PageType { NO_PAGE, LEAF_PAGE, INDEX_PAGE };

  volatile PageStatusUnion	m_PageStatus; // Status of this leaf page

  BtreeRootInternal*  m_Btree;			// Back pointer to root of the Btree
  PageType			  m_PageType;		// Leaf page or index page
  UINT				  m_PageSize;		// Page size in bytes (max 64K)
  volatile LONG       m_WastedSpace;    // Space wasted (in bytes) by records that have been deleted

  // The set of records on the page when it was created. 
  UINT16			  m_nSortedSet;		// Nr of records in the sorted set

public:
  BtBasePage(PageType type, UINT size, BtreeRootInternal* root)
  {
	_ASSERTE(size <= 64 * 1024);
	memset(this, 0, size);
	m_Btree = root;
	m_PageSize = size;
	m_PageType = type;
	m_nSortedSet = 0;
    m_WastedSpace = 0;
	m_PageStatus.m_Status.m_FirstFreeByte = size - 1;
	m_PageStatus.m_Status.m_nUnsortedReserved = 0;
	m_PageStatus.m_Status.m_PageState = PAGE_NORMAL;
	m_PageStatus.m_Status.m_SlotsCleared = 0;
  }

  bool IsLeafPage() { return m_PageType == LEAF_PAGE; }
  bool IsIndexPage() { return m_PageType == INDEX_PAGE; }

  int LiveRecordCount()
  {
      PageStatusUnion pst;
      pst.m_Status64 = m_PageStatus.m_Status64;

      int cnt =  m_nSortedSet + pst.m_Status.m_nUnsortedReserved - pst.m_Status.m_SlotsCleared;
      _ASSERTE(cnt >= 0);
      return cnt;
  }

  UINT16 PageSize() { return m_PageSize; }

  UINT KeySpaceSize() { return m_PageSize - m_PageStatus.m_Status.m_FirstFreeByte - 1;  }

  UINT UnusedSpace()
  {
      int unused = m_PageSize - KeySpaceSize() - m_nSortedSet*sizeof(KeyPtrPair);
      if (IsLeafPage())
      {
          unused -= m_PageStatus.m_Status.m_nUnsortedReserved*sizeof(KeyPtrPair);
      }
      _ASSERTE(unused >= 0);
      return UINT(unused);
  }

   virtual void PrintPage(FILE* file, UINT level) = 0;

   virtual UINT CheckPage(FILE* file, KeyType* lowBound, KeyType* hiBound) = 0;
};


// A leaf page has the following structure
// 1.  A fixed-size header area
// 2.  An array of record descriptors stored in sorted order on the record key.
//     These records were added to the page when the page was created. 
// 3.  An array of record descriptor for records inserted after the page was created.
//     The desciptors are stored in order of arrival and not in sorted order.
// 4.  An area for storing keys. This area grows backwards from the end of the page
//     towards the beginning of the page.
//     
// A record is deleted by clearing the record pointer field in its descriptor,
// i.e., deletions are done by marking.
//
class BtLeafPage : public BtBasePage
{
  friend class BtreeRootInternal;

   PermutationArray* m_PermArr;  // Array giving the sorted order of all elements

  KeyPtrPair	m_RecordArr[1];


  bool EnoughFreeSpace(UINT32 keylen, UINT64 pageState);

 
  KeyPtrPair* GetUnsortedEntry(UINT indx)
  {
	KeyPtrPair* ptr = nullptr;
	if (indx < m_PageStatus.m_Status.m_nUnsortedReserved)
	{
	  ptr = &m_RecordArr[indx + m_nSortedSet];
	}
	return ptr;
  }

  static UINT PageHeaderSize()
  {
	return sizeof(BtLeafPage) - sizeof(KeyPtrPair);
  }

  BTRESULT CreatePermutationArray();

public:
  // Constructor and destructor. What else did you expect?
  BtLeafPage(UINT32 size, BtreeRootInternal* root);
  ~BtLeafPage();

 
  KeyPtrPair* GetKeyPtrPair(UINT indx)
  {
      KeyPtrPair* ptr = nullptr;
      if (indx < UINT(m_nSortedSet + m_PageStatus.m_Status.m_nUnsortedReserved))
      {
          ptr = &m_RecordArr[indx];
      }
      return ptr;
  }

  BtreeRootInternal* GetBtreeRoot() { return m_Btree; }

  BTRESULT AddRecordToPage(KeyType* key, void* recptr);
 
  BTRESULT DeleteRecordFromPage(KeyType* key);

  BTRESULT SortUnsortedSet(KeyPtrPair* pSortedArr, UINT count);

  void ClosePage();

  BTRESULT ConsolidateLeafPage( BtLeafPage*& newpage, BtIterator* iter, UINT minFree);

  BTRESULT SplitLeafPage(BtIterator* iter);

  int KeySearchGE(char* searchKey, UINT keyLen);



  UINT CheckPage(FILE* file, KeyType* lowBound, KeyType* hiBound);
  void ShortPrint(FILE* file);
  void PrintPage(FILE* file, UINT level);

};

class BtIndexPage: public BtBasePage
{
  // Array of sorted index record entries 
  // that is, a separator and a pointer to and index or leaf page
  KeyPtrPair  m_RecordArr[1];

  UINT AppendToSortedSet(char* separator, UINT sepLen, void* ptr);
  static UINT PageHeaderSize()
  {
	return sizeof(BtIndexPage) - sizeof(KeyPtrPair);
  }

public:

  BtIndexPage(UINT size, BtreeRootInternal* root)
	: BtBasePage(INDEX_PAGE, size, root)
  {}

   UINT ComputePageSize(UINT entryCount, UINT keySpace)
  {
      return PageHeaderSize() + entryCount*sizeof(KeyPtrPair) + keySpace + 4;
  }

  KeyPtrPair* GetSortedEntry(UINT indx)
  {
	return (indx < m_nSortedSet) ? &m_RecordArr[indx] : nullptr;
  }
  
  
  int KeySearchGE(KeyType* searchKey);

  BTRESULT ExpandIndexPage(char* separator, UINT sepLen, BtBasePage* leftPage, BtBasePage* rightPage, UINT oldPos, BtIndexPage*& newPage);
  BTRESULT SplitIndexPage(BtIterator* iter);

  UINT CheckPage(FILE* file, KeyType* lowBound, KeyType* hiBound);

  void ShortPrint(FILE* file);
  void PrintPage(FILE* file, UINT level);
 
};

class BtreeRoot
{
protected:
  UINT32			  m_MinPageSize;
  UINT32			  m_MaxPageSize;
  double			  m_FreeSpaceFraction;

public:
  IMemoryAllocator*	  m_MemoryAllocator;
  CompareFn*		  m_CompareFn;			  // Key comparison function


  BtreeRoot()
  {
	m_MinPageSize = 128;
	m_MaxPageSize = 512;
	m_FreeSpaceFraction = 0.50;
	m_MemoryAllocator = GetDefaultMemoryAllocator();
	m_CompareFn = DefaultCompareKeys;

  }

  BTRESULT InsertRecord(KeyType* key, void* recptr);

  BTRESULT LookupRecord(KeyType* key, void*& recFound);

  BTRESULT DeleteRecord(KeyType* key);

  void Print(FILE* file);
 
};

class BtreeRootInternal : public BtreeRoot
{
    friend class BtLeafPage;
    friend class BtIndexPage;

    MemoryBroker*			  m_MemoryBroker;

    EpochManager*             m_EpochMgr;

    volatile BtBasePage*	  m_RootPage;

    // Statistics
    UINT                    m_nRecords;           // Nr of records 
    UINT                    m_nLeafPages;         // Nr of leaf pages
    UINT                    m_nIndexPages;        // Nr of index pages
    UINT                    m_nPageSplits;        // Nr of page splits
    UINT                    m_nConsolidations;    // Nr of page consolidations
    UINT                    m_nPageMerges;        // Nr of page merges

public:
    BtreeRootInternal()
    {
        m_MemoryBroker = new MemoryBroker(m_MemoryAllocator);
        m_EpochMgr = new EpochManager();
        m_EpochMgr->Initialize(m_MemoryBroker, this, nullptr);
        m_RootPage = nullptr;
        m_nRecords = m_nLeafPages = m_nIndexPages = 0;
        m_nPageSplits = m_nConsolidations = m_nPageMerges = 0;
    }

    // Compute the page size to allocate
    UINT ComputePageSize(UINT nrRecords, UINT keySpace, UINT minFree)
    {
        double spacePerRec = double(keySpace + nrRecords * sizeof(KeyPtrPair)) / double(nrRecords);
        UINT pageSize = BtLeafPage::PageHeaderSize() + UINT(spacePerRec*nrRecords*(1.0 + m_FreeSpaceFraction));
        INT  usedSpace = BtLeafPage::PageHeaderSize() + nrRecords*sizeof(KeyPtrPair) + keySpace;
        INT  freeSpace = pageSize - usedSpace;
        if (freeSpace < INT(minFree))
        {
            pageSize += minFree - freeSpace;
        }
        return pageSize;
    }

    BtBasePage* CreateEmptyPage();

    BtIndexPage* CreateIndexPage(BtBasePage* leftPage, BtBasePage* rightPage, char* separator, UINT sepLen);

    BTRESULT InsertRecordInternal(KeyType* key, void* recptr);

    BTRESULT LookupRecordInternal(KeyType* key, void*& recFound);

    BTRESULT DeleteRecordInternal(KeyType* key);

    BTRESULT FindTargetPage(KeyType* searchKey, BtIterator* iter);

    void CheckTree(FILE* file);
    void Print(FILE* file)
    {
	    BtBasePage* rootPage = const_cast<BtBasePage*>(m_RootPage);
	    if (rootPage)
	    {
	        rootPage->PrintPage(file, 1);
	    }
    }
  
};

class BtIterator
{
   friend class BtreeRootInternal;
   friend class BtLeafPage;
   friend class BtIndexPage;

   struct PathEntry
   {
       BtBasePage*  m_Page;
       int          m_Slot;
       UINT         m_RemSpace;
   };

  static const UINT MaxLevels = 10;

  BtreeRootInternal*	m_Btree;

  UINT					m_Count;			// No of entries added to m_Path
  PathEntry			    m_Path[MaxLevels];	// Pages on the path from the root to the current page

public:
  BtIterator(BtreeRootInternal* root=nullptr)
  {
	m_Btree = root;
	m_Count = 0;
    for (UINT i = 0; i < MaxLevels; i++)
    {
        m_Path[i].m_Page = nullptr;
        m_Path[i].m_Slot = 0;
    }
  }

  void Reset(BtreeRootInternal* root)
  {
      m_Btree = root;
      m_Count = 0;
  }

  void ExtendPath(BtBasePage* page, int slot)
  {
	m_Path[m_Count].m_Page = page;
    m_Path[m_Count].m_Slot = slot;
    m_Path[m_Count].m_RemSpace = page->UnusedSpace();
	m_Count++;
  }
};

