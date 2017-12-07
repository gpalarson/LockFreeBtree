#pragma once
#include <windows.h>
#include <atomic>
#include "mwCAS.h"
#include "Utilities.h"
#include "MemoryBroker.h"
#include "EpochManager.h"
#include "MemoryBroker.h"

using namespace std; 

//#define DO_LOG 1

// Forward references
class BtreeRootInternal;
class BtreePage;
class BtIterator;

enum BTRESULT {
  BT_SUCCESS, BT_INVALID_ARG, BT_DUPLICATE_KEY, BT_KEY_NOT_FOUND, BT_OUT_OF_MEMORY, BT_INTERNAL_ERROR,
  BT_PAGE_CLOSED, BT_PAGE_FULL, BT_NOT_INSERTED, BT_INSTALL_FAILED, BT_NO_MERGE
};

// Defaul functions used unless user specifies otherwise
IMemoryAllocator* GetDefaultMemoryAllocator();
int   DefaultCompareKeys(const void* key1, const int keylen1, const void* key2, const int keylen2);
using CompareFn = int(const void* key1, const int keylen1, const void* key2, const int keylen2);

struct TraceInfo
{
  static const int maxActions = 10;
  enum PageAction{ NO_ACTION, CONS_PAGE, SPLI_PAGE, MERGE_PAGE};
  struct Action
  {
	PageAction m_ActionType;
	bool	   m_WasInstalled;
	BtreePage* m_TrgtPage;
	BtreePage* m_ResPage1;
	BtreePage* m_ResPage2;
  };

    bool		m_DoRecord;
	BtreePage*  m_HomePage;
	int         m_HomePos;
	int         m_ActionCount;
	Action      m_ActionArr[maxActions];

	TraceInfo()
	{
	  m_DoRecord = true;
	  m_HomePage = nullptr;
	  m_HomePos = -1;
	  m_ActionCount = 0;
	  for (int i = 0; i < maxActions; i++)
	  {
		Action* pa = &m_ActionArr[i];
		pa->m_ActionType = NO_ACTION;
		pa->m_WasInstalled = false;
		pa->m_TrgtPage = pa->m_ResPage1 = pa->m_ResPage2 = nullptr;
	  }
	}

	int RecordAction(PageAction type, bool installed, BtreePage* trgt, BtreePage* res1, BtreePage* res2)
	{
	  if (m_ActionCount < maxActions)
	  {
		Action* pa = &m_ActionArr[m_ActionCount];
		pa->m_ActionType = type;
		pa->m_WasInstalled = installed;
		pa->m_TrgtPage = trgt;
		pa->m_ResPage1 = res1;
		pa->m_ResPage2 = res2;
	  }
	  m_ActionCount++;
	  return m_ActionCount;
	}

	void Print(FILE* file);

};


struct KeyType
{
    union
    {
		char*   m_pKeyValue;        // Pointer to key value
        UINT    m_oKeyValue;        // Offset to key value
    };
    UINT        m_KeyLen;           // Length in bytes

	TraceInfo* m_TrInfo;

  KeyType(char* key = nullptr, UINT keyLen = 0)
    {
        m_pKeyValue = key;
        m_KeyLen = keyLen;
		m_TrInfo = nullptr;
    }

    KeyType(UINT keyOffset, UINT keyLen)
    {
        m_oKeyValue = keyOffset;
        m_KeyLen = keyLen;
    }

    void GetKey(void*& key, UINT& keyLen)
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
// On a leaf page, it points to a B-tree record.

// Flag bit positions
// A BtreePtr field contains a pointer to a BtreePage on index pages and
// a pointer to a record on leaf pages.
// Bit 63 is used as a flag bit by MwCAS (to identify pointers to descriptors)
// Bit 62 is used as a flag to close the field, blocking further updates.
static const ULONG DescriptorFlagPos = 63;
static const ULONG ClosedFlagPos     = 62;
typedef  MwcTargetField<char*, DescriptorFlagPos>  BtreePtr;
static const ULONGLONG CloseBitMask = ULONGLONG(0x1) << ClosedFlagPos;

#pragma pack(push)
#pragma pack(2)

struct KeyPtrPair
{
 
  UINT16            m_KeyOffset;	  // Offset of the key from the beginning of the page
  UINT16            m_KeyLen;         // Key length in bytes.
  BtreePtr			m_Pointer;		  // Either a pointger to an index page or to a record 

  KeyPtrPair()
  {
	m_KeyOffset = 0;
	m_KeyLen = 0;
	m_Pointer = 0;
  }

  static bool IsClosedBitOn(ULONGLONG val)
  {
      return (val & CloseBitMask) != 0;
  }

  void Set(UINT16 keyoffset, UINT16 keylen, void* recptr)
  {
	m_KeyOffset = keyoffset;
	m_KeyLen = keylen; 
	m_Pointer = (char*)(recptr);
  }

  void CloseEntry()
  {
      LONGLONG oldVal = m_Pointer.ReadLL();
      LONGLONG newVal = oldVal | CloseBitMask;
	// Don't close entries already closed
	if ((oldVal & CloseBitMask) == 0)
      {
          InterlockedCompareExchange64((LONGLONG*)(&m_Pointer), newVal, oldVal);
      }
  }

  void CleanEntry()
  {
     LONGLONG oldVal = m_Pointer.ReadLL();
     LONGLONG newVal = oldVal & ~CloseBitMask;

      m_Pointer = (char*)(newVal);
  }

  bool IsClosed()
  {
      return (ULONGLONG(m_Pointer) & CloseBitMask) != 0;
  }

  bool IsDeleted()
  {
	LONGLONG val = m_Pointer.ReadLL();
	return( (val & ~CloseBitMask) == 0 );	
  }
};

static_assert(sizeof(KeyPtrPair) == 12, "Size of KeyPtrPair is not 12");
static_assert(sizeof(KeyPtrPair[10]) == 120, "Size of KeyPtrPair[10] is not 120");

using PStatusWord = MwcTargetField<void*, DescriptorFlagPos>;

#pragma pack(pop)

enum ePageState : UINT8 { PAGE_NORMAL, PAGE_CLOSING, PAGE_CLOSED, PAGE_INACTIVE};
enum ePendingAction: UINT8 {PA_NONE, PA_CONSOLIDATE, PA_SPLIT_PAGE, PA_MERGE_PAGE };

struct PageStatus
{
  // Note the high-order bit of m_PageState (bit 63) is used as a flag bit by MwCas
  ePageState	  m_PageState;			  // Page status
  ePendingAction  m_PendAction;			  // Action to be performed on the page
  UINT16		  m_nUnsortedReserved;	  // Leaf pages: no of record slots reserved in unsorted area.
  UINT16		  m_LastFreeByte;		  // Offset of the last free byte in the area for keys
  union
  {
	UINT16		  m_SlotsCleared;		  // Slots that have been cleared because its record was deleted.
											// Note that a reserved slot may be empty. A slot is empty if its record pointer field is null.
											// m_SlotsReserved - m_SlotsCleared is a safe upper bound on the number of actaul records on the page
 	UINT16		  m_UpdateCount;		  // Index pages: incremented when a page pointer is updated										
  };
  bool IsClosed() { return (m_PageState == PAGE_CLOSED || m_PageState == PAGE_CLOSING); }

  static LONGLONG MakePageInactive(LONGLONG curStatus)
  {
	LONGLONG newStatus = curStatus;
	PageStatus* newpst = (PageStatus*)(&newStatus);
	newpst->m_PageState = PAGE_INACTIVE;
	return newStatus;
  }

  static LONGLONG IncrUpdateCount(LONGLONG curStatus)
  {
	LONGLONG newStatus = curStatus;
	PageStatus* newpst = (PageStatus*)(&newStatus);
	newpst->m_UpdateCount++;
	return newStatus;
  }

  static void PrintPageStatus(FILE* file, LONGLONG psw)
  {
	PageStatus* pst = (PageStatus*)(&psw);
	char* state = nullptr;
	char* action = nullptr;
	switch (pst->m_PageState)
	{
	case PAGE_NORMAL:  state = "NORMAL"; break;
	case PAGE_CLOSING: state = "CLOSING"; break;
	case PAGE_CLOSED:  state = "CLOSED"; break;
	case PAGE_INACTIVE: state = "INACTIVE"; break;
	}
	switch (pst->m_PendAction)
	{
	case PA_NONE:  action = "NONE"; break;
	case PA_CONSOLIDATE: action = "CONSOLIDATE"; break;
	case PA_SPLIT_PAGE:  action = "SPLIT"; break;
	case PA_MERGE_PAGE:  action = "MERGE"; break;
	}
	fprintf(file, "Status: %s, %s, lastfree %hd, reserved %d, cleared/updates %hd ",
	  state, action, pst->m_LastFreeByte, pst->m_nUnsortedReserved, pst->m_SlotsCleared);
  }
};



// A permutation array is a mapping that specifies the sorted ordering of an array of RecordEntries
struct PermutationArray
{
  BtreePage*              m_TargetPage;      // back pointer to the leaf page
  LONGLONG				  m_CreateStatus;	 // page status when the permutation array was created
  UINT					  m_nrEntries;       // Nr of entries in the array
  UINT16				  m_PermArray[1];	 // The actual permutation array begins here

  PermutationArray(BtreePage* page, UINT count, LONGLONG status)
  {
    m_TargetPage = page;
	m_CreateStatus = status ;
    m_nrEntries = count;
    for (UINT i = 0; i < count; i++) 
	  m_PermArray[i] = i;
  }

  BTRESULT SortPermArray();

};

struct BtreeStatistics
{
  UINT	  m_LeafPages;
  UINT	  m_IndexPages;
  UINT	  m_Records;

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
	m_LeafPages = m_IndexPages = m_Records = 0;
	m_SpaceLP = m_AllocedSpaceLP = m_FreeSpaceLP = m_HeaderSpaceLP = m_KeySpaceLP = m_RecArrSpaceLP = m_DeletedSpaceLP = 0;
	m_SpaceIP = m_AllocedSpaceIP = m_HeaderSpaceIP = m_KeySpaceIP = m_RecArrSpaceIP = 0;
  }

};

class BtreePage
{
  friend class BtreeRootInternal;

  using UpdateCounter = MwcTargetField<volatile ULONGLONG, DescriptorFlagPos>;

public:
  enum CompType       { LT, LTE, EQ, GTE, GT};

protected:
  enum PageType:UINT8 { NO_PAGE, LEAF_PAGE, INDEX_PAGE };


  PStatusWord		  m_PageStatus; // Status of this leaf page

  BtreeRootInternal*  m_Btree;			// Back pointer to root of the Btree
#ifdef _DEBUG
  BtreePage*		  m_SrcPage1;		// Left or only source page
  BtreePage*          m_TrgtPage1;		// Left or only target page
  BtreePage*		  m_TrgtPage2;		// Right target page

  BtreePage*		  m_SrcPage2;		// Right source page
#endif
  PageType			  m_PageType;		// Leaf page or index page
  UINT16			  m_PageSize;		// Page size in bytes (max 64K)
  UINT16			  m_nSortedSet;		// Nr of records in the sorted set
  atomic_uint	      m_WastedSpace;    // Space wasted (in bytes) by records that have been deleted
  volatile PermutationArray* m_PermArr;      // Array giving the sorted order of all elements

  KeyPtrPair	      m_RecordArr[1];

  static UINT PageHeaderSize();
  UINT PageSize() { return m_PageSize; }
  UINT NetPageSize() { return m_PageSize - PageHeaderSize(); }
  UINT KeySpaceSize() {	
	LONGLONG stw = m_PageStatus.ReadLL();
	PageStatus* pst = (PageStatus*)(&stw);
	return m_PageSize - pst->m_LastFreeByte -1; 
  }
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

  void CloseLeafPage();

  BTRESULT TryToMergeLeafPage(BtIterator* iter);
  BTRESULT MergeLeafPages(BtreePage* otherPage, bool mergeOnRight, BtreePage** newPage);
  BTRESULT ConsolidateLeafPage(BtIterator* iter, UINT minFree);
  BTRESULT SplitLeafPage(BtIterator* iter);

  BTRESULT TryToMergeIndexPage(BtIterator* iter);
  BTRESULT MergeIndexPages(BtreePage* otherPage, bool mergeOnRight, BtreePage** newPage);
  BTRESULT ExpandIndexPage(char* separator, UINT sepLen, BtreePage* leftPage, BtreePage* rightPage, UINT oldPos, BtreePage*& newPage);
  BTRESULT ShrinkIndexPage(UINT dropPos, BtreePage*& newIndexPage);
  BTRESULT SplitIndexPage(BtIterator* iter);

  BTRESULT CreatePermutationArray(PermutationArray*& permArr);
 
public:
	BtreePage(PageType type, UINT size, BtreeRootInternal* root);

	bool IsLeafPage() { return m_PageType == LEAF_PAGE; }
	bool IsIndexPage() { return m_PageType == INDEX_PAGE; }
	bool IsClosed()    
	{ 
	  LONGLONG psw = m_PageStatus.ReadLL();
	  PageStatus* pst = (PageStatus*)(&psw);
	  return (pst->m_PageState == PAGE_CLOSED);  
	} 

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
	void PrintPage(FILE* file, UINT level, bool recursive=true);
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

  BTRESULT TraceRecord(KeyType* key);

  void Print(FILE* file);
  void CheckTree(FILE* file);
  void PrintStats(FILE* file);

};

class BtreeRootInternal : public BtreeRoot
{
  friend class BtreePage;

    MemoryBroker*			m_MemoryBroker;
    EpochManager*           m_EpochMgr;
	BtreePtr				m_RootPage;

	// Tree status statistics
	atomic_uint             m_nRecords;           // Nr of records 
    atomic_uint             m_nLeafPages;         // Nr of leaf pages
    atomic_uint             m_nIndexPages;        // Nr of index pages
 
    // Dynamic statistics
	atomic_uint				m_nInserts;			  // Nr of records inserted
	atomic_uint				m_nDeletes;			  // Nr of records deleted
	atomic_uint				m_nUpdates;			  // Nr of records updated.
    atomic_uint             m_nPageSplits;        // Nr of page splits
    atomic_uint             m_nConsolidations;    // Nr of page consolidations
    atomic_uint             m_nPageMerges;        // Nr of page merges

	// List of pages that were not installed
	volatile BtreePage*		m_FailList;


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

	BTRESULT TraceRecordInternal(KeyType* key);

	void ClearTreeStats();
	void PrintTreeStats(FILE* file);

	void CheckTree(FILE* file);
	void Print(FILE* file);

};

// Class storing the path taken when descending from the root to a leaf page
struct PathEntry
{
  UINT				m_Level;		  // Index level of this node ( root page is at level 1)
  BtreePage*		m_Page;			  // Page in this level
  PStatusWord		m_PageStatus;	  // Page status

  int				m_Slot;			  // Slot in m_RecArray where search stopped
  char*             m_Bound;		  // Separator in that slot
  UINT				m_BoundLen;		  // Length of the separator
  BtreePage*		m_NextPage;		  // Pointer to index page on next level
 


  void PrintPathEntry(FILE* file, UINT level)
  {
	fprintf(file, "Level %d: page 0X%I64X with status ", level, LONGLONG(m_Page));
	PageStatus::PrintPageStatus(file, LONGLONG(m_PageStatus));
	fprintf(file, "\n         chose slot %d, separator %.*s, pointer 0X%I64X\n",
	           m_Slot, m_BoundLen, m_Bound, LONGLONG(m_NextPage));
	m_Page->PrintPage(file, level, false);
  }
};

class BtIterator
{
   friend class BtreeRootInternal;
  friend class BtreePage;

  static const UINT MaxLevels = 10;

  BtreeRootInternal*	m_Btree;
  TraceInfo*            m_TrInfo;

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
 		m_Path[i].m_PageStatus = 0;

       m_Path[i].m_Slot = 0;
		m_Path[i].m_NextPage = nullptr;
		m_Path[i].m_Bound = 0;
		m_Path[i].m_BoundLen = 0;
    }
  }

  void Reset(BtreeRootInternal* root)
  {
      m_Btree = root;
      m_Count = 0;
  }

  void ExtendPath(BtreePage* page, int slot, LONGLONG pageStatus64)
  {
	m_Path[m_Count].m_Page = page;
    m_Path[m_Count].m_Slot = slot;
	m_Path[m_Count].m_PageStatus = (void*)(pageStatus64);
	if (slot >= 0)
	{
	  KeyPtrPair* kp = page->GetKeyPtrPair(slot);
	  m_Path[m_Count].m_Bound = (char*)(page)+page->GetKeyPtrPair(slot)->m_KeyOffset, kp->m_KeyLen;
	  m_Path[m_Count].m_BoundLen = kp->m_KeyLen;
	  m_Path[m_Count].m_NextPage = (BtreePage*)(kp->m_Pointer.ReadPP());
	}
	m_Count++;
  }

  void PrintPath(FILE* file)
  {
	fprintf(file, "******* Path from root down ********\n");
	for (UINT i = 0; i < m_Count; i++)
	{
	  m_Path[i].PrintPathEntry(file, i + 1);
	}
	fprintf(file, "******* End of path ********\n");
  }
};

#ifdef DO_LOG
struct LogRec;

struct InsertInfo
{
  char*		  m_Key;
  UINT		  m_KeyLen;
  BtreePage*  m_Page;
  LONGLONG	  m_Status;
  PathEntry   m_Prior;

  static void RecInsert(char BorE, char* key, UINT keylen, BtreePage* page, LONGLONG pstatus, PathEntry* prior);
 
  void Print(FILE* file)
  {
	fprintf(file, "%.*s, page 0X%I64X ", m_KeyLen, m_Key, ULONGLONG(m_Page));
	PageStatus::PrintPageStatus(file, m_Status);
	fprintf(file, "\n                            ");
	fprintf(file, "Index term: %.*s ", m_Prior.m_BoundLen, m_Prior.m_Bound);
	fprintf(file, "page 0X%I64X slot %d", ULONGLONG(m_Prior.m_Page), m_Prior.m_Slot);
	PageStatus::PrintPageStatus(file, LONGLONG(m_Prior.m_PageStatus));
	fprintf(file, "\n");
  }
};

struct ConsInfo
{
  BtreePage*  m_TrgPage;
  LONGLONG	  m_TrgtStatus;
  BtreePage*  m_NewPage;
  LONGLONG	  m_NewStatus;
  UINT		  m_RecCount;

  static void RecCons(char BorE, BtreePage* trgt, LONGLONG status, BtreePage* newpage, LONGLONG newstatus, UINT reccount);
 
  void Print(FILE* file)
  {
	fprintf(file, "page 0X%I64X, ", ULONGLONG(m_TrgPage));
	PageStatus::PrintPageStatus(file, m_TrgtStatus);
	fprintf(file, " recs %d, new 0X%I64X ", m_RecCount, ULONGLONG(m_NewPage));
	PageStatus::PrintPageStatus(file, m_NewStatus);
  }
};

struct SplitInfo
{
  BtreePage*	m_SrcPage;
  LONGLONG		m_SrcStatus;
  BtreePage*	m_LeftPage;
  BtreePage*	m_RightPage;

  static void RecSplit(char BorE, BtreePage* srcpage, LONGLONG srcstate, BtreePage* left, BtreePage* right);
 
  void Print(FILE* file)
  {
	fprintf(file, "page 0X%I64X ", ULONGLONG(m_SrcPage));
	PageStatus::PrintPageStatus(file, m_SrcStatus);
	fprintf(file, "left 0X%I64X, right 0X%I64X ", ULONGLONG(m_LeftPage), ULONGLONG(m_RightPage));
  }
};


#define MaxLogEntries 100000
extern volatile LONGLONG EventCounter;
extern LogRec EventLog[];
extern void PrintLog(FILE* file, int eventCount);


struct LogRec
{
  UINT64	  m_Time;
  UINT		  m_ThreadId;
  char		  m_Action;
  char		  m_BeginEnd;
  union
  {
	InsertInfo m_Insert;
	ConsInfo   m_Cons;
	SplitInfo  m_Split;
  };

  LogRec()
  {
	m_Time = 0;
	m_ThreadId = 0;
	m_Action = ' ';
	m_BeginEnd = ' ';
  }

  void Print(FILE* file)
  {
	fprintf(file, "Event %I64d by %d: %c,%c ", m_Time, m_ThreadId, m_Action, m_BeginEnd);
	switch (m_Action)
	{
	case 'I': m_Insert.Print(file); break;
	case 'C': m_Cons.Print(file); break;
	case 'S': m_Split.Print(file); break;
	default:
	  fprintf(file, "Unkwown event type %c", m_Action);
	}
	fprintf(file, "\n");
  }
 
 };
#endif DO_LOG



