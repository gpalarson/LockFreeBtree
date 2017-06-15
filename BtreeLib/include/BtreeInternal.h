#pragma once
#include <windows.h>
#include <atomic>
#include "mwCAS.h"
#include "Utilities.h"
#include "MemoryBroker.h"
#include "EpochManager.h"
#include "MemoryBroker.h"

using namespace std; 

// Forward references
class BtreeRootInternal;
class BtreePage;
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

  bool IsClosed() { return (m_Status.m_PageState == PAGE_CLOSED || m_Status.m_PageState == PAGE_CLOSING); }
};

// A permutation array is a mapping that specifies the sorted ordering of an array of RecordEntries
struct PermutationArray
{
  BtreePage*             m_TargetPage;      // back pointer to the leaf page
  PageStatusUnion		  m_CreateStatus;	 // page status when the permutation array was created
  UINT					  m_nrEntries;       // Nr of entries in the array
  UINT16				  m_PermArray[1];	 // The actual permutation array begins here

  PermutationArray(BtreePage* page, UINT count, PageStatusUnion* status)
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

class BtreePage
{
  friend class BtreeRootInternal;

public:
  enum CompType       { LT, LTE, EQ, GTE, GT};

protected:
  enum PageType:UINT8 { NO_PAGE, LEAF_PAGE, INDEX_PAGE };


  volatile PageStatusUnion	m_PageStatus; // Status of this leaf page

  BtreeRootInternal*  m_Btree;			// Back pointer to root of the Btree
  PageType			  m_PageType;		// Leaf page or index page
  UINT16			  m_PageSize;		// Page size in bytes (max 64K)
  UINT16			  m_nSortedSet;		// Nr of records in the sorted set
  volatile LONG       m_WastedSpace;    // Space wasted (in bytes) by records that have been deleted
  volatile PermutationArray*   m_PermArr;        // Array giving the sorted order of all elements
  KeyPtrPair	      m_RecordArr[1];

  static UINT PageHeaderSize();
  UINT PageSize() { return m_PageSize; }
  UINT NetPageSize() { return m_PageSize - PageHeaderSize(); }
  UINT KeySpaceSize() { return m_PageSize - m_PageStatus.m_Status.m_LastFreeByte - 1; }
  UINT SortedSetSize() { return m_nSortedSet; }
  void LiveRecordSpace(UINT& liveRecs, UINT& keySpace);
  UINT LiveRecordCount();

  bool EnoughFreeSpace(UINT32 keylen, UINT64 pageState);
  
  KeyPtrPair* GetUnsortedEntry(UINT indx) { return GetKeyPtrPair(indx + m_nSortedSet); }

  UINT AppendToSortedSet(char* separator, UINT sepLen, void* ptr);
  BTRESULT SortUnsortedSet(KeyPtrPair* pSortedArr, UINT count);

  BTRESULT ExtractLiveRecords(KeyPtrPair*& liveRecArray, UINT& count, UINT& keySpace);
  BTRESULT AddRecordToPage(KeyType* key, void* recptr);
  BTRESULT DeleteRecordFromPage(KeyType* key);
 BTRESULT CopyToNewPage(BtreePage* newPage);

  void ClosePage();

  BTRESULT TryToMergeLeafPage(BtIterator* iter);
  BTRESULT MergeLeafPages(BtreePage* otherPage, bool mergeOnRight, BtreePage** newPage);
  BTRESULT ConsolidateLeafPage(BtIterator* iter, UINT minFree);
  BTRESULT SplitLeafPage(BtIterator* iter);

  BTRESULT TryToMergeIndexPage(BtIterator* iter);
  BTRESULT MergeIndexPages(BtreePage* otherPage, bool mergeOnRight, BtreePage** newPage);
  BTRESULT ExpandIndexPage(char* separator, UINT sepLen, BtreePage* leftPage, BtreePage* rightPage, UINT oldPos, BtreePage*& newPage);
  BTRESULT ShrinkIndexPage(UINT dropPos, BtreePage*& newIndexPage);
  BTRESULT SplitIndexPage(BtIterator* iter);

  BTRESULT CreatePermutationArray();
 
public:
	BtreePage(PageType type, UINT size, BtreeRootInternal* root);

	bool IsLeafPage() { return m_PageType == LEAF_PAGE; }
	bool IsIndexPage() { return m_PageType == INDEX_PAGE; }
	bool IsClosed()    { return (m_PageStatus.m_Status.m_PageState == PAGE_CLOSED);  } 

	BtreeRootInternal* GetBtreeRoot() { return m_Btree; }
	KeyPtrPair* GetKeyPtrPair(UINT indx);
	int KeySearch(KeyType* searchKey, BtreePage::CompType ctype, bool forwardScan = true);
	UINT FreeSpace();

	void DeletePage(BtIterator* iter);

	UINT CheckPage(FILE* file, KeyType* lowBound, KeyType* hiBound);
	UINT CheckLeafPage(FILE* file, KeyType* lowBound, KeyType* hiBound);

	void ComputeTreeStats(BtreeStatistics* statsp);
	void ComputeLeafStats(BtreeStatistics* statsp);

	void ShortPrint(FILE* file);
	void ShortPrintLeaf(FILE* file);
	void PrintPage(FILE* file, UINT level);
	void PrintLeafPage(FILE* file, UINT level);

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

struct KeyComparisonMap
{
  // Maps result from comparison function to bool
  // First entry is for the case when the comparison return a negative value,
  // the second for when it returns a zero, and
  // the third for when it returns a positive value
  struct CompOutcome { bool m_Result[3]; };

  CompOutcome m_Row[5];

  KeyComparisonMap()
  {
	m_Row[BtreePage::LT] = { true, false, false };
	m_Row[BtreePage::LTE] = { true, true, false };
	m_Row[BtreePage::EQ] = { false, true, false };
	m_Row[BtreePage::GTE] = { false, true, true };
	m_Row[BtreePage::GT] = { false, false, true };
  }

  bool CompResult(BtreePage::CompType ct, int cv)
  {
	UINT indx = 0;
	if (cv < 0) indx = 0; else if (cv == 0) indx = 1; else indx = 2;
	return m_Row[ct].m_Result[indx];
  }
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
  void CheckTree(FILE* file);
  void PrintStats(FILE* file);

};

class BtreeRootInternal : public BtreeRoot
{
  friend class BtreePage;

    MemoryBroker*			m_MemoryBroker;
    EpochManager*           m_EpochMgr;
    volatile BtreePage*		m_RootPage;

	// Tree status stats
	atomic_uint             m_nRecords;           // Nr of records 
    atomic_uint             m_nLeafPages;         // Nr of leaf pages
    atomic_uint             m_nIndexPages;        // Nr of index pages
 
    // Dynamic statistics
	UINT					m_nInserts;			  // Nr of records inserted
	UINT					m_nDeletes;			  // Nr of records deleted
	UINT					m_nUpdates;			  // Nr of records updated.
     UINT                   m_nPageSplits;        // Nr of page splits
    UINT                    m_nConsolidations;    // Nr of page consolidations
    UINT                    m_nPageMerges;        // Nr of page merges


	// Compute the page size to allocate. 
	UINT ComputeLeafPageSize(UINT nrRecords, UINT keySpace, UINT minFree);
	UINT ComputeIndexPageSize(UINT fanout, UINT keySpace);

	BTRESULT FindTargetPage(KeyType* searchKey, BtIterator* iter);
	BTRESULT AllocateLeafPage(UINT recCount, UINT keySpace, BtreePage*& newPage);
	BTRESULT AllocateIndexPage(UINT recCount, UINT keySpace, BtreePage*& newPage);


	BtreePage* CreateIndexPage(BtreePage* leftPage, BtreePage* rightPage, char* separator, UINT sepLen);

	BTRESULT InstallNewPages(BtIterator* iter, BtreePage* leftPage, BtreePage* rightPage, char* separator, UINT sepLen);
	void ComputeTreeStats(BtreeStatistics* statsp);

public:
	BtreeRootInternal();

	BTRESULT InsertRecordInternal(KeyType* key, void* recptr);
	BTRESULT LookupRecordInternal(KeyType* key, void*& recFound);
	BTRESULT DeleteRecordInternal(KeyType* key);

	void ClearTreeStats();
	void PrintTreeStats(FILE* file);

	void CheckTree(FILE* file);
	void Print(FILE* file);

};

// Class storing the path taken when descending from the root to a leaf page
class BtIterator
{
   friend class BtreeRootInternal;
  friend class BtreePage;

   struct PathEntry
   {
       BtreePage*		m_Page;
       int				m_Slot;
	   PageStatusUnion	m_PageStatus;
       UINT				m_RemSpace;
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
		m_Path[i].m_PageStatus.m_Status64 = 0;
		m_Path[i].m_RemSpace = 0;
    }
  }

  void Reset(BtreeRootInternal* root)
  {
      m_Btree = root;
      m_Count = 0;
  }

  void ExtendPath(BtreePage* page, int slot, UINT64 pageStatus64)
  {
	m_Path[m_Count].m_Page = page;
    m_Path[m_Count].m_Slot = slot;
	m_Path[m_Count].m_PageStatus.m_Status64 = pageStatus64;
	m_Path[m_Count].m_RemSpace = page->FreeSpace(); 
	m_Count++;
  }
};

