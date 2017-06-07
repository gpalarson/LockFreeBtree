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
class BtBasePage;
class BtIterator;

enum BTRESULT {
  BT_SUCCESS, BT_INVALID_ARG, BT_DUPLICATE_KEY, BT_KEY_NOT_FOUND, BT_OUT_OF_MEMORY, BT_INTERNAL_ERROR,
  BT_PAGE_CLOSED, BT_PAGE_FULL, BT_NOT_INSERTED, BT_MAX_SIZE, BT_MIN_SIZE, BT_INSTALL_FAILED, BT_NO_MERGE
};

// Defaul functions used unless user specifies otherwise
IMemoryAllocator* GetDefaultMemoryAllocator();
int   DefaultCompareKeys(const void* key1, const int keylen1, const void* key2, const int keylen2);

using CompareFn = int(const void* key1, const int keylen1, const void* key2, const int keylen2);

struct KeyType
{
    union
    {
	char*   m_pKeyValue;        // Pointer to key value
        UINT    m_oKeyValue;        // Offset to key value
    };
    UINT        m_KeyLen;           // Length in bytes

  KeyType(char* key = nullptr, UINT keyLen = 0)
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

  static void GetMinValue(char*& value, UINT& len)
    {
	value = (char*)(&MinChar);
        len = 1;
    }

  static void GetMaxValue(char*& value, UINT& len)
    {
	value = (char*)(&MaxChar);
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

  static const ULONGLONG CloseBitMask = ULONGLONG(0x1) << 63;


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
	// Don't close entries for deleted rows or entries already closed
	if (oldVal != 0 && (oldVal & CloseBitMask) == 0)
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

  bool IsDeleted()
  {
	ULONGLONG val = ULONGLONG(const_cast<void*>(m_RecPtr));
	return( (val & ~CloseBitMask) == 0 );	
  }
};

#pragma pack(pop)

enum ePageState : UINT16 { PAGE_NORMAL, PAGE_CLOSING, PAGE_CLOSED, SPLIT_PAGE, MERGE_PAGE };

struct PageStatus
{
  ePageState	  m_PageState;			  // Page status
  UINT16		  m_nUnsortedReserved;	  // Number of record slots that have been reserved
										  // in the unsorted area
  UINT16		  m_LastFreeByte;		  // Offset of the last free byte in the area for keys
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
  BtBasePage*             m_TargetPage;      // back pointer to the leaf page
  PageStatusUnion		  m_CreateStatus;	 // page status when the permutation array was created
  UINT					  m_nrEntries;       // Nr of entries in the array
  UINT16				  m_PermArray[1];	 // The actual permutation array begins here

  PermutationArray(BtBasePage* page, UINT count, PageStatusUnion* status)
  {
      m_TargetPage = page;
	m_CreateStatus.m_Status64 = status->m_Status64;
      m_nrEntries = count;
      for (UINT i = 0; i < count; i++) m_PermArray[i] = i;
  }

  BTRESULT SortPermArray();

};

struct BtreeStatistics
{
  UINT	  m_LeafPages;
  UINT	  m_IndexPages;

  UINT	  m_SpaceLP;
  UINT	  m_AllocedSpaceLP;
  UINT	  m_FreeSpaceLP;
  UINT	  m_HeaderSpaceLP;
  UINT	  m_KeySpaceLP;
  UINT	  m_RecArrSpaceLP;
  UINT	  m_DeletedSpaceLP;

  UINT	  m_SpaceIP;
  UINT	  m_AllocedSpaceIP;
  UINT	  m_HeaderSpaceIP;
  UINT	  m_KeySpaceIP;
  UINT	  m_RecArrSpaceIP;

  void Clear()
  {
	m_LeafPages = m_IndexPages = 0;
	m_SpaceLP = m_AllocedSpaceLP = m_FreeSpaceLP = m_HeaderSpaceLP = m_KeySpaceLP = m_RecArrSpaceLP = m_DeletedSpaceLP = 0;
	m_SpaceIP = m_AllocedSpaceIP = m_HeaderSpaceIP = m_KeySpaceIP = m_RecArrSpaceIP = 0;
  }

};

class BtBasePage
{
  friend class BtreeRootInternal;
 
protected:
  enum PageType:UINT8 { NO_PAGE, LEAF_PAGE, INDEX_PAGE };

  volatile PageStatusUnion	m_PageStatus; // Status of this leaf page

  BtreeRootInternal*  m_Btree;			// Back pointer to root of the Btree
  PageType			  m_PageType;		// Leaf page or index page
  UINT16			  m_PageSize;		// Page size in bytes (max 64K)
  UINT16			  m_nSortedSet;		// Nr of records in the sorted set
  volatile LONG       m_WastedSpace;    // Space wasted (in bytes) by records that have been deleted

  PermutationArray*   m_PermArr;        // Array giving the sorted order of all elements

  KeyPtrPair	      m_RecordArr[1];

  UINT AppendToSortedSet(char* separator, UINT sepLen, void* ptr);

  static UINT PageHeaderSize()
  {
      BtBasePage dummy(LEAF_PAGE, 1, nullptr);
      UINT headerSize = UINT((char*)(&dummy.m_RecordArr[0]) - (char*)(&dummy));
      return headerSize;
  }

  BTRESULT MergeIndexPages(BtBasePage* otherPage, bool mergeOnRight, BtBasePage** newPage);

  BTRESULT CreatePermutationArray();

 
public:
    BtBasePage(PageType type, UINT size, BtreeRootInternal* root);

    bool IsLeafPage() { return m_PageType == LEAF_PAGE; }
    bool IsIndexPage() { return m_PageType == INDEX_PAGE; }
    bool IsClosed()    { return (m_PageStatus.m_Status.m_PageState == PAGE_CLOSED);  } 

    KeyPtrPair* GetKeyPtrPair(UINT indx)
    {
        KeyPtrPair* ptr = nullptr;
        if (indx < UINT(m_nSortedSet + m_PageStatus.m_Status.m_nUnsortedReserved))
        {
            ptr = &m_RecordArr[indx];
        }
        return ptr;
    }

    UINT UnusedSpace()
    {
        UINT used = PageHeaderSize() + KeySpaceSize() + (m_nSortedSet + m_PageStatus.m_Status.m_nUnsortedReserved) * sizeof(KeyPtrPair);
        _ASSERTE(used <= m_PageSize);
        return UINT(m_PageSize - used);
    }


  BtreeRootInternal* GetBtreeRoot() { return m_Btree; }
 
  int KeySearchGE(KeyType* searchKey);

  void LiveRecordSpace(UINT& liveRecs, UINT& keySpace);
  UINT LiveRecordCount()
  {
      PageStatusUnion pst;
      pst.m_Status64 = m_PageStatus.m_Status64;
      return m_nSortedSet + pst.m_Status.m_nUnsortedReserved - pst.m_Status.m_SlotsCleared;
  }

  // Returns total space for keys, including deleted ones
  UINT KeySpaceSize()  { return m_PageSize - m_PageStatus.m_Status.m_LastFreeByte - 1; }
  UINT SortedSetSize() { return m_nSortedSet; }
  UINT16 PageSize()    { return m_PageSize; }

  void DeletePage(BtIterator* iter);

  BTRESULT ExpandIndexPage(char* separator, UINT sepLen, BtBasePage* leftPage, BtBasePage* rightPage, UINT oldPos, BtBasePage*& newPage);
  BTRESULT ShrinkIndexPage(UINT dropPos, BtBasePage*& newIndexPage);
  BTRESULT SplitIndexPage(BtIterator* iter);
  BTRESULT TryToMergeIndexPage(BtIterator* iter);

  UINT CheckPage(FILE* file, KeyType* lowBound, KeyType* hiBound);
  void ComputeTreeStats(BtreeStatistics* statsp);
  void ShortPrint(FILE* file);
  void PrintPage(FILE* file, UINT level);

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

  static UINT LeafPageHeaderSize()
  {
	BtLeafPage dummy(1, nullptr);
	UINT headerSize = UINT((char*)(&dummy.m_RecordArr[0]) - (char*)(&dummy));
	return headerSize;
  }

  UINT NetPageSize() { return m_PageSize - LeafPageHeaderSize(); }

  BTRESULT ExtractLiveRecords(KeyPtrPair*& liveRecArray, UINT& count, UINT& keySpace);
 
  BTRESULT CopyToNewPage(BtLeafPage* newPage);

  BTRESULT TryToMergeLeafPage(BtIterator* iter);

public:
  // Constructor and destructor. What else did you expect?
  BtLeafPage(UINT32 size, BtreeRootInternal* root);
  ~BtLeafPage();

  BTRESULT AddRecordToPage(KeyType* key, void* recptr);

  BTRESULT DeleteRecordFromPage(KeyType* key);

  BTRESULT SortUnsortedSet(KeyPtrPair* pSortedArr, UINT count);

 
  void ClosePage();

  BTRESULT ConsolidateLeafPage(BtIterator* iter, UINT minFree);

  BTRESULT SplitLeafPage(BtIterator* iter);

  // Merge two leaf pages: this and otherPage. 
  BTRESULT MergeLeafPages(BtLeafPage* otherPage, bool mergeOnRight, BtLeafPage** newPage);


  UINT CheckPage(FILE* file, KeyType* lowBound, KeyType* hiBound);
  void ComputeTreeStats(BtreeStatistics* statsp);
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

  void CheckTree(FILE* f);
  void PrintStats(FILE* file);

};

class BtreeRootInternal : public BtreeRoot
{
  friend class BtBasePage;
    friend class BtLeafPage;

    MemoryBroker*			  m_MemoryBroker;

    EpochManager*             m_EpochMgr;

    volatile BtBasePage*	  m_RootPage;

    // Statistics
  UINT					m_nInserts;			  // Nr of records inserted
  UINT					m_nDeletes;			  // Nr of records deleted
  UINT					m_nUpdates;			  // Nr of records updated.
    UINT                    m_nRecords;           // Nr of records 
    UINT                    m_nLeafPages;         // Nr of leaf pages
    UINT                    m_nIndexPages;        // Nr of index pages
    UINT                    m_nPageSplits;        // Nr of page splits
    UINT                    m_nConsolidations;    // Nr of page consolidations
    UINT                    m_nPageMerges;        // Nr of page merges

public:
  BtreeRootInternal();

    // Compute the page size to allocate
  UINT ComputeLeafPageSize(UINT nrRecords, UINT keySpace, UINT minFree);
  UINT ComputeIndexPageSize(UINT fanout, UINT keySpace);

  void ClearStats()
    {
	m_nInserts = m_nDeletes = m_nUpdates = 0;
	m_nPageSplits = m_nConsolidations = m_nPageMerges = 0;
        }

  BtBasePage* CreateEmptyLeafPage();

    BtBasePage* CreateIndexPage(BtBasePage* leftPage, BtBasePage* rightPage, char* separator, UINT sepLen);

  BTRESULT InstallNewPages(BtIterator* iter, BtBasePage* leftPage, BtBasePage* rightPage, char* separator, UINT sepLen);


    BTRESULT InsertRecordInternal(KeyType* key, void* recptr);

    BTRESULT LookupRecordInternal(KeyType* key, void*& recFound);

    BTRESULT DeleteRecordInternal(KeyType* key);

    BTRESULT FindTargetPage(KeyType* searchKey, BtIterator* iter);

  void ComputeTreeStats(BtreeStatistics* statsp);

    void CheckTree(FILE* file);
  void Print(FILE* file);
  void PrintTreeStats(FILE* file);
};

// Class storing the path taken when descending from the root to a leaf page
class BtIterator
{
   friend class BtreeRootInternal;
  friend class BtBasePage;
   friend class BtLeafPage;

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
  BtIterator(BtreeRootInternal* root = nullptr)
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
	m_Path[m_Count].m_RemSpace = (page->IsLeafPage())? ((BtLeafPage*)(page))->UnusedSpace(): 0;
	m_Count++;
  }
};

