#include <Windows.h>
#include "MemoryAllocator.h"
#include "MemoryBroker.h"
#include "mwCAS.h"
#include "BtreeInternal.h"

// Initialize an empty leaf page
BtLeafPage::BtLeafPage(UINT32 size, BtreeRootInternal* root)
  : BtBasePage(BtBasePage::LEAF_PAGE, size, root)
{
  m_PermArr = nullptr;
}


BtLeafPage::~BtLeafPage()
{
  // Clear page
  m_PageStatus.m_Status64 = 0;
  m_nSortedSet = 0;
}

int BtBasePage::KeySearchGE(KeyType* searchKey)
{
       CreatePermutationArray();

     // Do a linear serch - use this later for testing binary search.
    char* baseAddr = (char*)(this);
 
    int indx = -1;
    KeyPtrPair* pre = nullptr;
    char* curKey = nullptr;
    int cv = 0;
    for (UINT pos = 0; UINT(pos) < m_PermArr->m_nrEntries; pos++)
    {
        indx = m_PermArr->m_PermArray[pos];
        pre = GetKeyPtrPair(indx);
        curKey = baseAddr + pre->m_KeyOffset;
        cv = m_Btree->m_CompareFn(searchKey->m_pKeyValue, searchKey->m_KeyLen, curKey, pre->m_KeyLen);
        if (IsIndexPage())
        {
            if (cv <= 0)
            {
                break;
            }
        }
        else
        {
            if (cv < 0)
            {
                indx--;
                break;
            }
            if (cv == 0)
            {
                break;
            }
        }
    }
    return indx;
}

INT BinarySearchEq(const char *pSearchKey, const UINT keyLen, const char*baseAddr, const KeyPtrPair* rgRecord, const USHORT cLen, CompareFn* CompareKeys)
{
    // do a binary search for the desired key
    INT Low = 0, High = cLen - 1;
    INT Middle, Cmp;
    char* curKey = nullptr;

    while (Low <= High) 
    {
        Middle = (Low + High) / 2;
        curKey = const_cast<char*>(baseAddr) + rgRecord[Middle].m_KeyOffset;
        Cmp = CompareKeys(curKey, rgRecord[Middle].m_KeyLen, pSearchKey, keyLen);
        if (Cmp < 0) 
        {
            Low = Middle + 1;
        }else
        if (Cmp > 0) 
        {
            High = Middle - 1;
        }
        else 
        {
            return Middle;
        }
    }
    return -1;
}

// Find first key that is greater than or equal to the search key
INT BinarySearchGE(const char *pSearchKey, const UINT keyLen, const char*baseAddr, const KeyPtrPair* rgRecord, const USHORT cLen, CompareFn* CompareKeys, bool& fEqual)

{
    if (cLen == 0)
    {
        return 0;
    }

    // do a binary search for the lowest key greater 
    // than or equal to the search key
    INT Low = 0, High = cLen;
    fEqual = false;

    INT Middle, Cmp, Trgt = cLen;
    char* curKey = nullptr;

    while (High - Low > 3) {
        Middle = (Low + High) / 2;
        curKey = const_cast<char*>(baseAddr) + rgRecord[Middle].m_KeyOffset;
        Cmp = CompareKeys(pSearchKey, keyLen, curKey, rgRecord[Middle].m_KeyLen );

        if (Cmp > 0) {
            // we now know that lowkey < searchkey
            Low = Middle;
        }
        else {
            // we now know that searchkey <= highkey
            High = Middle;
            Trgt = High;
            fEqual = (Cmp == 0);
        }
    }
    // we are now down to at most three keys
    for (INT k = Low; k<High; ++k) {
        curKey = const_cast<char*>(baseAddr) + rgRecord[k].m_KeyOffset;
        Cmp = CompareKeys(pSearchKey, keyLen, curKey, rgRecord[k].m_KeyLen );
        if (Cmp <= 0) {
            Trgt = k;
            fEqual = (Cmp == 0);
            break;
        }
    }

    return Trgt;
}

BTRESULT BtBasePage::CreatePermutationArray()
{
tryagain:

    PageStatusUnion initStatus;
    initStatus.m_Status64 = m_PageStatus.m_Status64;
    BTRESULT hr = BT_SUCCESS;

    PermutationArray* permArr = m_PermArr;

    // Do we already have a permutation array?
    if (permArr)
    {
        // We do. Is it still valid?
        if (permArr->m_CreateStatus.m_Status64 == initStatus.m_Status64)
        {
            return hr;
        }
        m_PermArr = nullptr;
        m_Btree->m_EpochMgr->Deallocate(permArr, MemObjectType::TmpPointerArray);
    }

    // Need to create a new permutation array
    UINT count = m_nSortedSet + initStatus.m_Status.m_nUnsortedReserved;
    UINT size = sizeof(PermutationArray) + (count - 1)*sizeof(UINT16);
    HRESULT hre =  m_Btree->m_MemoryBroker->Allocate(size, (void**)&permArr, MemObjectType::TmpPointerArray);
    new(permArr) PermutationArray(this, count, &initStatus);
    if (initStatus.m_Status.m_nUnsortedReserved > 0)
    {
        hr = permArr->SortPermArray();
    }

    // Has new records been added since we filled the permutation array? If so, try again.
    if (initStatus.m_Status.m_nUnsortedReserved < m_PageStatus.m_Status.m_nUnsortedReserved)
    {
         m_Btree->m_EpochMgr->DeallocateNow(permArr, MemObjectType::TmpPointerArray);
        goto tryagain;
    }

    m_PermArr = permArr;

    return hr;

}

void BtLeafPage::ComputeTreeStats(BtreeStatistics* statsp)
{
  statsp->m_LeafPages++;
  statsp->m_SpaceLP += m_PageSize;
  statsp->m_AllocedSpaceLP += UINT(_msize(this));
  statsp->m_HeaderSpaceLP += LeafPageHeaderSize();
  statsp->m_KeySpaceLP += KeySpaceSize();
  statsp->m_RecArrSpaceLP += (m_nSortedSet+m_PageStatus.m_Status.m_nUnsortedReserved) * sizeof(KeyPtrPair);
  statsp->m_FreeSpaceLP += UnusedSpace();
  statsp->m_DeletedSpaceLP += m_WastedSpace;
  UINT hsize = UINT((char*)(&m_RecordArr[0]) - (char*)(this));
  _ASSERTE(hsize == LeafPageHeaderSize());
}

UINT BtLeafPage::CheckPage(FILE* file, KeyType* lowBound, KeyType* hiBound)
{
    KeyPtrPair* pre = nullptr;
    char* curKey = nullptr;
    UINT  curKeyLen = 0;
    char* prevKey = nullptr;
    UINT  prevKeyLen = 0;
    UINT  errorCount = 0;

    for (UINT i = 0; i < UINT(m_nSortedSet + m_PageStatus.m_Status.m_nUnsortedReserved); i++)
    {
        pre = GetKeyPtrPair(i);
        curKey = (pre->m_KeyLen > 0) ? (char*)(this) + pre->m_KeyOffset : nullptr;
        curKeyLen = pre->m_KeyLen;
        if (prevKey && curKey && i<m_nSortedSet)
        {
            int cv = m_Btree->m_CompareFn(prevKey, prevKeyLen, curKey, curKeyLen);
            if (cv > 0)
            {
                fprintf(file, "Keys in wrong order: \"%1.*s\", \"%1.*s\"\n", prevKeyLen, prevKey, curKeyLen, curKey);
                errorCount++;
            }
        }
        if (curKey)
        {
            int cv = m_Btree->m_CompareFn(curKey, curKeyLen, lowBound->m_pKeyValue, lowBound->m_KeyLen);
            if (cv < 0)
            {
                fprintf(file, "Key less than lower bound: \"%1.*s\", \"%1.*s\"\n", curKeyLen, curKey, lowBound->m_KeyLen, (char*)(lowBound->m_pKeyValue));
                errorCount++;
            }

            cv = m_Btree->m_CompareFn(curKey, curKeyLen, hiBound->m_pKeyValue, hiBound->m_KeyLen);
            if (cv > 0)
            {
                fprintf(file, "Key greater than upper bound: \"%1.*s\", \"%1.*s\"\n", curKeyLen, curKey, hiBound->m_KeyLen, (char*)(hiBound->m_pKeyValue));
                errorCount++;
            }
        }

    }

	// Check permutatiom array
	if (m_PermArr)
	{
	  if (m_PageStatus.m_Status64 == m_PermArr->m_CreateStatus.m_Status64)
	  {
		_ASSERTE(m_PermArr->m_TargetPage == this);
		_ASSERTE(m_PermArr->m_nrEntries == m_nSortedSet + m_PageStatus.m_Status.m_nUnsortedReserved);
	  }
	}

    if (errorCount > 0)
    {
        PrintPage(stdout, 0);
    }
 
    return errorCount;
}

void BtLeafPage::ShortPrint(FILE* file)
{
    fprintf(file, " LP@0x%llx, %dB, %d+%d", UINT64(this), m_PageSize, m_nSortedSet, m_PageStatus.m_Status.m_nUnsortedReserved);

	if (m_nSortedSet + m_PageStatus.m_Status.m_nUnsortedReserved > 0)
	{
    KeyPtrPair* pre = GetKeyPtrPair(0);    
    UINT lowKeyLen = pre->m_KeyLen; 
    UINT hiKeyLen  = pre->m_KeyLen;
    char* lowKey   = (char*)(this) + pre->m_KeyOffset;
    char* hiKey    = (char*)(this) + pre->m_KeyOffset;

	  for (UINT i = 1; i < UINT(m_nSortedSet + m_PageStatus.m_Status.m_nUnsortedReserved); i++)
    {
        pre = GetKeyPtrPair(i);
        char* curKey = (char*)(this) + pre->m_KeyOffset;
        int cv = m_Btree->m_CompareFn(curKey, pre->m_KeyLen, lowKey, lowKeyLen);
        if (cv < 0)
        {
            lowKey = curKey;
            lowKeyLen = pre->m_KeyLen;
        }
        cv = m_Btree->m_CompareFn(curKey, pre->m_KeyLen, hiKey, hiKeyLen);
        if (cv > 0)
        {
            hiKey = curKey;
            hiKeyLen = pre->m_KeyLen;
        }
    }

    fprintf(file, " \"%1.*s\",", lowKeyLen, lowKey);
    fprintf(file, " \"%1.*s\" ", hiKeyLen, hiKey);
}
	else
	{
	  fprintf(file, "empty");
	}
}

void BtLeafPage::PrintPage(FILE* file, UINT level)
{
  fprintf(file, "\n------ Leaf page -----------------------\n");

  UINT nUnsorted = m_PageStatus.m_Status.m_nUnsortedReserved;
  UINT delSpace = 0;
  for (UINT i = 0; i < m_nSortedSet ; i++)
  {
	KeyPtrPair* pre = GetKeyPtrPair(i);
	if (!pre->m_RecPtr)
	{
	  delSpace += pre->m_KeyLen + sizeof(KeyPtrPair);
	}
  }

  UINT arrSpace = (m_nSortedSet + nUnsorted) * sizeof(KeyPtrPair);
  UINT keySpace = m_PageSize - m_PageStatus.m_Status.m_LastFreeByte -1;
  UINT32 freeSpace = m_PageSize - LeafPageHeaderSize() - arrSpace - keySpace;
  
  fprintf(file, "Size %d, Usage:  hdr %d array(%d+%d) %d keys %d free %d deleted %d\n", 
	m_PageSize, LeafPageHeaderSize(), m_nSortedSet, nUnsorted, arrSpace, keySpace, freeSpace, delSpace);
  fprintf(file, "State: %d, reserved %d, cleared %d, first free %d\n", 
	m_PageStatus.m_Status.m_PageState, m_PageStatus.m_Status.m_nUnsortedReserved,
	m_PageStatus.m_Status.m_SlotsCleared, m_PageStatus.m_Status.m_LastFreeByte);

  char* baseAddr = (char*)(this);

  fprintf(file, "%d records in sorted area\n", m_nSortedSet);
  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pe = GetKeyPtrPair(i);
	fprintf(file, "  (%d, %d, 0x%llx):", pe->m_KeyOffset, pe->m_KeyLen, ULONGLONG(pe->m_RecPtr));
	char* keyPtr = (baseAddr + pe->m_KeyOffset);
	char* recPtr = (pe->m_RecPtr) ? (char*)(pe->m_RecPtr) : ".....................";
	fprintf(file, "  \"%1.*s\", \"%s\"\n", pe->m_KeyLen, keyPtr, recPtr);
  }

  fprintf(file, "%d records in unsorted area\n", nUnsorted);
  for (UINT i = 0; i < nUnsorted; i++)
  {
	KeyPtrPair* pe = GetUnsortedEntry(i);
	fprintf(file, "  (%d, %d, 0x%llx):", pe->m_KeyOffset, pe->m_KeyLen, ULONGLONG(pe->m_RecPtr));
	char* keyPtr = (baseAddr + pe->m_KeyOffset);
	char* recPtr = (pe->m_RecPtr) ? (char*)(pe->m_RecPtr) : ".....................";
	fprintf(file, "  \"%1.*s\", \"%s\"\n", pe->m_KeyLen, keyPtr, recPtr);
  }

  fprintf(file, "\n----------------------------------------\n");
}

void BtBasePage::ShortPrint(FILE* file)
{
    fprintf(file, " IP@0x%llx, %dB, %d", UINT64(this), m_PageSize, m_nSortedSet);
    KeyPtrPair* pre = GetKeyPtrPair(0);
    char* sep = (char*)(this) + pre->m_KeyOffset;
    fprintf(file, " \"%1.*s\",", pre->m_KeyLen, sep);
	if (m_nSortedSet >= 2)
	{
        pre = GetKeyPtrPair(m_nSortedSet-2);
        sep = (char*)(this) + pre->m_KeyOffset;
        fprintf(file, " \"%1.*s\" ", pre->m_KeyLen, sep);
    }
}

void BtBasePage::ComputeTreeStats(BtreeStatistics* statsp)
{
  statsp->m_IndexPages++;
  statsp->m_SpaceIP += m_PageSize;
  statsp->m_AllocedSpaceIP += UINT(_msize(this));
  statsp->m_HeaderSpaceIP += PageHeaderSize();
  statsp->m_KeySpaceIP += KeySpaceSize();
  statsp->m_RecArrSpaceIP += m_nSortedSet * sizeof(KeyPtrPair);

  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pre = GetKeyPtrPair(i);
	BtBasePage* page = (BtBasePage*)(const_cast<void*>(pre->m_RecPtr));
	if (page->IsIndexPage())
	{
	  BtBasePage* ip = (BtBasePage*)(page);
	  ip->ComputeTreeStats(statsp);
	}
	else
	{
	  BtLeafPage* lp = (BtLeafPage*)(page);
	  lp->ComputeTreeStats(statsp);
	}
  }

}

UINT BtBasePage::CheckPage(FILE* file, KeyType* lowBound, KeyType* hiBound)
{
    if (IsLeafPage())
    {
        ((BtLeafPage*)(this))->CheckPage(file, lowBound, hiBound);
    }

    KeyPtrPair* pre = nullptr; 
    char* curKey = nullptr;
    UINT  curKeyLen = 0;
    char* prevKey = nullptr;
    UINT  prevKeyLen = 0;
    UINT  errorCount = 0;
 
     for (UINT i = 0; i < m_nSortedSet; i++)
    {
        pre = GetKeyPtrPair(i);
        curKey = ( pre->m_KeyLen > 0)? (char*)(this) + pre->m_KeyOffset : nullptr ; 
        curKeyLen = pre->m_KeyLen;
        if (prevKey && curKey)
        {
            int cv = m_Btree->m_CompareFn(prevKey, prevKeyLen, curKey, curKeyLen);
            if (cv > 0)
            {
                fprintf(file, "Separators in wrong order: \"%1.*s\", \"%1.*s\"\n", prevKeyLen, prevKey, curKeyLen, curKey);
                errorCount++;
            }
        }
        if (curKey)
        {
            int cv = m_Btree->m_CompareFn(curKey, curKeyLen, lowBound->m_pKeyValue, lowBound->m_KeyLen);
            if (cv < 0)
            {
                fprintf(file, "Separators less than lower bound: \"%1.*s\", \"%1.*s\"\n", curKeyLen, curKey,lowBound->m_KeyLen, (char*)(lowBound->m_pKeyValue) );
                errorCount++;
            }

            cv = m_Btree->m_CompareFn(curKey, curKeyLen, hiBound->m_pKeyValue, hiBound->m_KeyLen);
            if (cv > 0)
            {
                fprintf(file, "Separators greater than upper bound: \"%1.*s\", \"%1.*s\"\n", curKeyLen, curKey, hiBound->m_KeyLen, (char*)(hiBound->m_pKeyValue) );
                errorCount++;
            }      
        }

    }

    if (errorCount > 0)
    {
        PrintPage(file, 0);
    }

    if (IsIndexPage())
    {

        // Recursively check the subtrees rooted on this page
        KeyType lb;
        KeyType hb;

        lb = *lowBound;
        for (UINT i = 0; i < m_nSortedSet; i++)
        {
            pre = GetKeyPtrPair(i);
            curKey = (pre->m_KeyLen > 0) ? (char*)(this) + pre->m_KeyOffset : nullptr;
            curKeyLen = pre->m_KeyLen;
            new(&hb)KeyType(curKey, curKeyLen);

            if (pre->m_RecPtr)
            {
                BtBasePage* pg = (BtBasePage*)(pre->m_RecPtr);
                errorCount += pg->CheckPage(file, &lb, &hb);
            }

            lb = hb;
        }
    }
    return errorCount;
}


void BtBasePage::PrintPage(FILE* file, UINT level)
{
    if (IsLeafPage())
    {
        ((BtLeafPage*)(this))->PrintPage(file, level);
        return;
    }
  fprintf(file, "\n------ Index page at level %d -----------------\n", level);

  UINT nUnsorted = m_PageStatus.m_Status.m_nUnsortedReserved;
  UINT delSpace = 0;
  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pre = GetKeyPtrPair(i);
	if (!pre->m_RecPtr)
	{
	  delSpace += pre->m_KeyLen + sizeof(KeyPtrPair);
	}
  }

  UINT arrSpace = (m_nSortedSet + nUnsorted) * sizeof(KeyPtrPair);
  UINT keySpace = m_PageSize - m_PageStatus.m_Status.m_LastFreeByte -1;
  UINT32 freeSpace = m_PageSize - PageHeaderSize() - arrSpace - keySpace;

  fprintf(file, "Size %d, Usage:  hdr %d array(%d+%d) %d keys %d free %d deleted %d\n",
	m_PageSize, PageHeaderSize(), m_nSortedSet, nUnsorted, arrSpace, keySpace, freeSpace, delSpace);
  fprintf(file, "State: %d, reserved %d, cleared %d, first free %d\n",
	m_PageStatus.m_Status.m_PageState, m_PageStatus.m_Status.m_nUnsortedReserved,
	m_PageStatus.m_Status.m_SlotsCleared, m_PageStatus.m_Status.m_LastFreeByte);

  char* baseAddr = (char*)(this);

  fprintf(file, "%d records in sorted area\n", m_nSortedSet);
  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pe = GetKeyPtrPair(i);
	fprintf(file, "  (%d, %d, 0x%llx):", pe->m_KeyOffset, pe->m_KeyLen, ULONGLONG(pe->m_RecPtr));
   
	char* keyPtr =( pe->m_KeyLen > 0)? (baseAddr + pe->m_KeyOffset): "----------------";
    fprintf(file, " \"%1.*s\",", pe->m_KeyLen, keyPtr);

    BtBasePage* bp = (BtBasePage*)(pe->m_RecPtr);
    if (bp->IsIndexPage()) ((BtBasePage*)(bp))->ShortPrint(file);
    if (bp->IsLeafPage()) ((BtBasePage*)(bp))->ShortPrint(file);
    fprintf(file, "\n");
  }

  fprintf(file, "\n----------------------------------------\n");

  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pe = GetKeyPtrPair(i);
    BtBasePage* pg = (BtBasePage*)(pe->m_RecPtr);
    if (pg->IsIndexPage()) ((BtBasePage*)(pg))->PrintPage(file, level+1);
    //if (pg->IsLeafPage()) ((BtLeafPage*)(pg))->ShortPrint(file);
  }
}


void BtreeRoot::Print(FILE* file)
{
  BtreeRootInternal* root = (BtreeRootInternal*)(this);
  root->Print(file);

}

BtBasePage* BtreeRootInternal::CreateEmptyLeafPage()
{
  BtLeafPage* page = nullptr;
  HRESULT hre = m_MemoryBroker->Allocate(m_MinPageSize, (void**)(&page), MemObjectType::LeafPage);
  if (page)
  {
	new(page) BtLeafPage(m_MinPageSize, this);
  }

  return page;
}

BtBasePage* BtreeRootInternal::CreateIndexPage(BtBasePage* leftPage, BtBasePage* rightPage, char* separator, UINT sepLen)
{
    KeyType hibound;
    KeyType::GetMaxValue(hibound.m_pKeyValue, hibound.m_KeyLen);  

    UINT keySpace = sepLen + hibound.m_KeyLen;
    UINT pageSize = ComputeIndexPageSize(2, keySpace);

    BtBasePage* page = nullptr;
    HRESULT hr = m_MemoryBroker->Allocate(pageSize, (void**)(&page), MemObjectType::IndexPage);
    if (hr != S_OK)
    {
        //btr = (hr == E_OUTOFMEMORY) ? BT_OUT_OF_MEMORY : BT_INTERNAL_ERROR;
        page = nullptr; // Just to make sure
        goto exit;
    }
    new(page) BtBasePage(BtBasePage::INDEX_PAGE, pageSize, this);

    // Store separator key first and then the high bound
    page->AppendToSortedSet(separator, sepLen, leftPage);
    page->AppendToSortedSet(hibound.m_pKeyValue, hibound.m_KeyLen, rightPage);
    _ASSERTE(page->UnusedSpace() == 0);

 exit:
  return page;
}

#ifdef NOTUSED
// Return index of the entry with the smallest key equal to or greater than the search key
int BtIndexPage::KeySearchGE(KeyType* searchKey )
{
  // On index pages, an entry points to a page containing records with keys that are less than or equal to the separator
  //_ASSERTE(m_nSortedSet >= 2);

  char* baseAddr = (char*)(this); 

  // Do a linear serch - use this later for testing binary search.
  int pos = -1;
  KeyPtrPair* pre = nullptr;
  char* curKey = nullptr;
  int cv = 0;
  for (pos = 0; UINT(pos) < UINT(m_nSortedSet-1); pos++)
  {
	pre = GetKeyPtrPair(pos); 
	curKey = baseAddr + pre->m_KeyOffset;
	cv = m_Btree->m_CompareFn(searchKey->m_pKeyValue, searchKey->m_KeyLen, curKey, pre->m_KeyLen);
	if (cv <= 0) 
	{
		break;
	 }
  }
  return pos;


#ifdef NOTYET
  // We now know that searchKey > sortArr[1].m_Key and < sortArr[count-1].m_Key
  UINT lowIndx = 0;
  UINT hiIndx = count - 1;
  UINT midIndx = 0;
  while (lowIndx < hiIndx)
  {
	midIndx = (lowIndx + hiIndx) >> 1;
	pre = &searchArr[midIndx];
	curKey = baseAddr + pre->m_KeyOffset;
	cv = m_CompareFn(searchKey, keyLen, curKey, pre->m_KeyLen);
	if (cv < 0) hiIndx = midIndx;
	if (cv >= 0) lowIndx = midIndx;
  }
#endif
  
}
#endif

BtBasePage::BtBasePage(PageType type, UINT size, BtreeRootInternal* root)
{
    _ASSERTE(size <= 64 * 1024);
    memset(this, 0, size);
    m_Btree = root;
    m_PageSize = size;
    m_PageType = type;
    m_nSortedSet = 0;
    m_WastedSpace = 0;
    m_PageStatus.m_Status.m_LastFreeByte = size - 1;
    m_PageStatus.m_Status.m_nUnsortedReserved = 0;
    m_PageStatus.m_Status.m_PageState = PAGE_NORMAL;
    m_PageStatus.m_Status.m_SlotsCleared = 0;
    m_PermArr = nullptr;
}

UINT BtBasePage::AppendToSortedSet(char* key, UINT keyLen, void* ptr)
{
    _ASSERTE(UnusedSpace() >= keyLen + sizeof(KeyPtrPair));
    m_PageStatus.m_Status.m_LastFreeByte -= keyLen;
    char* dst = (char*)(this) + m_PageStatus.m_Status.m_LastFreeByte + 1;
    memcpy_s(dst, keyLen, key, keyLen);

    m_nSortedSet++;
    KeyPtrPair* pre = GetKeyPtrPair(m_nSortedSet - 1);
    pre->Set(m_PageStatus.m_Status.m_LastFreeByte + 1, keyLen, ptr);

    return m_nSortedSet;
}



// When a leaf page or index page is split, we create a new parent page with room for one new separator-pointer pair.
// The separator equals the highest key value on the left page.
// leftPage and rightPage are the new pages that were created by the split.
// In the old parent page, the split page was reference in position oldPos.
BTRESULT BtBasePage::ExpandIndexPage(char* separator, UINT sepLen, BtBasePage* leftPage, BtBasePage* rightPage, UINT oldPos, BtBasePage*& newPage)
{
    _ASSERTE(IsIndexPage());

    BTRESULT btr = BT_SUCCESS;
    UINT pageSize = m_Btree->ComputeIndexPageSize(m_nSortedSet + 1, KeySpaceSize() + sepLen);
    BtBasePage* newpage = nullptr;
    HRESULT hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&newpage), MemObjectType::IndexPage);
    if (hr != S_OK) 
    { 
        btr = (hr == E_OUTOFMEMORY) ? BT_OUT_OF_MEMORY : BT_INTERNAL_ERROR;
        goto exit; 
    }
    new(newpage)BtBasePage(INDEX_PAGE, pageSize, m_Btree);

    // Allow the parent page to be larger than the max page size temporararily
    // but mark it to be split
    if (pageSize >= m_Btree->m_MaxPageSize)
    {
       newpage->m_PageStatus.m_Status.m_PageState = SPLIT_PAGE;
    }

    char* curSep = nullptr;
    UINT curSepLen = 0;
    KeyPtrPair* pre = nullptr;

    // spos it position in the source (old) page
    // tpos is position in the target (new) page
    UINT spos = 0;
    for (UINT tpos = 0; tpos < UINT(m_nSortedSet+1); tpos++)
    {
        if (tpos != oldPos)
        {
            pre = GetKeyPtrPair(spos);
            curSep = (char*)(this) + pre->m_KeyOffset;
            newpage->AppendToSortedSet(curSep, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
            spos++;
        }
        else
        {
            newpage->AppendToSortedSet(separator, sepLen, leftPage);
        }
    }
    // The entry in OldPos+1 points to the old page (the one that got split)
    // so we must set it to point to the corresponding new page
    newpage->GetKeyPtrPair(oldPos + 1)->m_RecPtr = rightPage;

    _ASSERTE(newpage->UnusedSpace() == 0);

    newPage = newpage;

exit:
    return btr;
}


BTRESULT BtBasePage::SplitIndexPage(BtIterator* iter)
{
    _ASSERTE(IsIndexPage());

    BTRESULT btr = BT_SUCCESS;
    UINT lCount = m_nSortedSet / 2;
    UINT rCount = m_nSortedSet - lCount;

    // Copy first lCount records to the left new page (lower keys)
    UINT keySpace = 0;
    for (UINT i = 0; i < lCount; i++) keySpace += GetKeyPtrPair(i)->m_KeyLen;
    UINT pageSize = m_Btree->ComputeIndexPageSize(lCount, keySpace);

    BtBasePage* leftPage = nullptr;
    HRESULT hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&leftPage), MemObjectType::LeafPage);
    if (hr != S_OK) 
    { 
        btr = (hr == E_OUTOFMEMORY) ? BT_OUT_OF_MEMORY : BT_INTERNAL_ERROR;
        goto exit; 
    }
    new(leftPage)BtBasePage(INDEX_PAGE, pageSize, m_Btree);

    for (UINT i = 0; i < lCount; i++)
    {
        KeyPtrPair* pre = GetKeyPtrPair(i);
        char* sep = (char*)(this) + pre->m_KeyOffset;
		leftPage->AppendToSortedSet(sep, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
    }
    _ASSERTE(leftPage->UnusedSpace() == 0);


    // Copy the higher rCount records into the right new page (higher keys)
    keySpace = 0;
    for (UINT i = lCount; i < m_nSortedSet; i++) keySpace += GetKeyPtrPair(i)->m_KeyLen;
    pageSize = m_Btree->ComputeIndexPageSize(rCount, keySpace);

    BtBasePage* rightPage = nullptr;
    hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&rightPage), MemObjectType::LeafPage);
    if (hr != S_OK) 
    { 
       btr = (hr == E_OUTOFMEMORY) ? BT_OUT_OF_MEMORY : BT_INTERNAL_ERROR;
       goto exit; 
    }
    new(rightPage)BtBasePage(INDEX_PAGE, pageSize, m_Btree);

    for (UINT i = lCount; i < m_nSortedSet; i++)
    {
        KeyPtrPair* pre = GetKeyPtrPair(i);
        char* sep = (char*)(this) + pre->m_KeyOffset;
		rightPage->AppendToSortedSet(sep, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
    }
    _ASSERTE(rightPage->UnusedSpace() == 0);

     // Use the last key of the right page as separator for the two pages.
    // A separator thus indicates the highest key value allowed on a page.
    // The separator will be added to the parent index page.
    char* separator = (char*)(leftPage)+leftPage->GetKeyPtrPair(lCount - 1)->m_KeyOffset;
    UINT seplen = leftPage->GetKeyPtrPair(lCount - 1)->m_KeyLen;

	// Install the two new pages
	btr = m_Btree->InstallNewPages(iter, leftPage, rightPage, separator, seplen);
	if (btr == BT_INSTALL_FAILED)
	{
	  m_Btree->m_EpochMgr->Deallocate(leftPage, MemObjectType::IndexPage);
	  m_Btree->m_EpochMgr->Deallocate(rightPage, MemObjectType::IndexPage);
	}
	else
	{
	  m_Btree->m_nPageSplits++;
	  m_Btree->m_nIndexPages++;
	}
    
#ifdef DISABLED
    fprintf(stdout, "\n=== Splitting index page ========= \n");
    fprintf(stdout, "  === source: ");
    //ShortPrint(stdout);
    PrintPage(stdout, iter->m_Count);
    fprintf(stdout, "\n  ===   left: ");
    //leftPage->ShortPrint(stdout);
    leftPage->PrintPage(stdout, iter->m_Count);
    fprintf(stdout, "\n              Separator: %1.*s\n", seplen, separator);
    fprintf(stdout, "  ===  right: ");
    //rightPage->ShortPrint(stdout);
    rightPage->PrintPage(stdout, iter->m_Count);
    fprintf(stdout, "\n===================================\n");
#endif

exit:
    return btr;
}

BTRESULT BtBasePage::TryToMergeIndexPage(BtIterator* iter)
{
    _ASSERTE(IsIndexPage());
  BTRESULT btr = BT_SUCCESS;

  BtBasePage* newPage = nullptr;

  UINT myCount = 0, myKeySpace = 0;
  UINT leftCount = 0, leftKeySpace = 0;
  UINT rightCount = 0, rightKeySpace = 0;

  BtBasePage* parent = (BtBasePage*)(iter->m_Path[iter->m_Count - 2].m_Page);
  UINT mySlot = iter->m_Path[iter->m_Count - 2].m_Slot;

  myCount = m_nSortedSet;
  myKeySpace = KeySpaceSize();

  BtBasePage* leftPage = nullptr;
  BtBasePage* rightPage = nullptr;

  if (mySlot > 0)
  {
	leftPage = (BtBasePage*)(parent->GetKeyPtrPair(mySlot - 1)->m_RecPtr);
	leftCount = leftPage->m_nSortedSet;
	leftKeySpace = leftPage->KeySpaceSize();
  }
  if (mySlot + 1 < parent->SortedSetSize())
  {
	rightPage = (BtBasePage*)(parent->GetKeyPtrPair(mySlot + 1)->m_RecPtr);
	rightCount = rightPage->m_nSortedSet;
	rightKeySpace = rightPage->KeySpaceSize();
  }

  UINT slotToDelete = 0;
  enum mergeType { NONE, LEFT, RIGHT } mergeDir;
  mergeDir = NONE;
  if (leftPage && rightPage)
  {
	// OK to merge either way
	mergeDir = (leftCount * sizeof(KeyPtrPair) + leftKeySpace < rightCount * sizeof(KeyPtrPair) + rightKeySpace) ? LEFT : RIGHT;
  }
  else
	if (leftPage)
	{
	  // OK to merge left
	  mergeDir = LEFT;
	}
	else
	  if (rightPage)
	  {
		// OK to merge right
		mergeDir = RIGHT;
	  }


  // Merge with the smaller neighbouring page
  if (mergeDir == LEFT)
  {
	// Merge with left
	UINT pageSize = m_Btree->ComputeIndexPageSize(leftCount + myCount, leftKeySpace + myKeySpace);
	if (pageSize <= m_Btree->m_MaxPageSize)
	{
	  btr = MergeIndexPages(leftPage, false, &newPage);
	  rightPage = nullptr;
	  slotToDelete = mySlot - 1;
	}

  }
  else
	if (mergeDir == RIGHT)
	{
	  // Merge with right
	  UINT pageSize = m_Btree->ComputeLeafPageSize(rightCount + myCount, rightKeySpace + myKeySpace, 0);
	  if (pageSize <= m_Btree->m_MaxPageSize)
	  {
		btr = MergeIndexPages(rightPage, true, &newPage);
		leftPage = nullptr;
		slotToDelete = mySlot;
	  }
	}
	else
	{
	  leftPage = nullptr;
	  rightPage = nullptr;
	  newPage = nullptr;
	  btr = BT_NO_MERGE;
	}

  BtBasePage* newParent = nullptr;
  if (newPage)
  {

	// Create new parent index page

	parent->ShrinkIndexPage(slotToDelete, newParent);
	KeyPtrPair* myNewEntry = newParent->GetKeyPtrPair(slotToDelete);
	myNewEntry->m_RecPtr = newPage;

	// Determine where to install the new page
	BtBasePage** installAddr = nullptr;
	int parentIndx = iter->m_Count - 2;
	_ASSERTE(parentIndx >= 0);

	if (parentIndx == 0)
	{
	  // The b-tree object is the grandparent
	  installAddr = const_cast<BtBasePage**>(&m_Btree->m_RootPage);
	}
	else
	{
	  BtBasePage* grandParentPage = (BtBasePage*)(iter->m_Path[parentIndx - 1].m_Page);
	  KeyPtrPair* recEntry = grandParentPage->GetKeyPtrPair(iter->m_Path[parentIndx - 1].m_Slot);
	  installAddr = (BtBasePage**)(&recEntry->m_RecPtr);
	}

	LONG64 oldval = InterlockedCompareExchange64((LONG64*)(installAddr), LONG64(newParent), LONG64(parent));
	if (oldval != LONG64(parent))
	{
	  btr = BT_INSTALL_FAILED;
	  goto exit;
	}
	InterlockedIncrement(&m_Btree->m_nPageMerges);
  }


exit:
  if (btr != BT_SUCCESS)
  {
	if (newPage) m_Btree->m_MemoryBroker->DeallocateNow(newPage, MemObjectType::IndexPage);
	newPage = nullptr;
	if (newParent) m_Btree->m_MemoryBroker->DeallocateNow(newParent, MemObjectType::IndexPage);
	newParent = nullptr;
  }
  else
  {
	if (leftPage)  m_Btree->m_MemoryBroker->Free(leftPage, MemObjectType::IndexPage);
	if (rightPage) m_Btree->m_MemoryBroker->Free(rightPage, MemObjectType::LeafPage);
	InterlockedDecrement(&m_Btree->m_nIndexPages);
	newPage->m_Btree->m_MemoryBroker->Free(this, MemObjectType::IndexPage);
  }

  return btr;

}


BTRESULT BtBasePage::MergeIndexPages(BtBasePage* otherPage, bool mergeOnRight, BtBasePage** newPage)
{

    _ASSERTE(IsIndexPage());
  BTRESULT btr = BT_SUCCESS;

  BtBasePage* leftPage = nullptr;
  BtBasePage* rightPage = nullptr;
  BtBasePage* newIndexPage = nullptr;
  *newPage = nullptr;

  if (mergeOnRight)
  {
	leftPage = this;
	rightPage = otherPage;
  }else
  {
	leftPage = otherPage;
	rightPage = this;
  }

  UINT keySpace = leftPage->KeySpaceSize() + rightPage->KeySpaceSize();
  UINT recCount = leftPage->LiveRecordCount() + rightPage->LiveRecordCount();
  UINT pageSize = m_Btree->ComputeLeafPageSize(recCount, keySpace, 0);
  HRESULT hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&newIndexPage), MemObjectType::IndexPage);
  if (hr != S_OK)
  {
	btr = BT_OUT_OF_MEMORY;
	goto exit;
  }
  new(newIndexPage)BtBasePage(INDEX_PAGE, pageSize, m_Btree);

  // Insert the records into the new page
  for (UINT i = 0; i < leftPage->m_nSortedSet; i++)
  {
	KeyPtrPair* pre = leftPage->GetKeyPtrPair(i);
	char* key = (char*)(leftPage) + pre->m_KeyOffset;
	newIndexPage->AppendToSortedSet(key, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
  }
  for (UINT i = 0; i < rightPage->m_nSortedSet; i++)
  {
	KeyPtrPair* pre = rightPage->GetKeyPtrPair(i);
	char* key = (char*)(rightPage) + pre->m_KeyOffset;
	newIndexPage->AppendToSortedSet(key, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
  }
  *newPage = newIndexPage;

exit:

  if (btr != BT_SUCCESS)
  {
	if (newIndexPage) m_Btree->m_MemoryBroker->DeallocateNow(newIndexPage, MemObjectType::LeafPage);
	newIndexPage = nullptr;
  }

  return btr;
}

BTRESULT BtBasePage::ShrinkIndexPage(UINT dropPos, BtBasePage*& newIndexPage)
{
  _ASSERTE(IsIndexPage());

  BTRESULT btr = BT_SUCCESS;
  BtBasePage* newpage = nullptr;

  if (m_nSortedSet <= 1)
  {
	goto exit;
  }

  _ASSERTE(dropPos < m_nSortedSet);
  UINT sepLen = m_RecordArr[dropPos].m_KeyLen;
  UINT pageSize = m_Btree->ComputeIndexPageSize(m_nSortedSet -1, KeySpaceSize() - sepLen);

  HRESULT hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&newpage), MemObjectType::IndexPage);
  if (hr != S_OK)
  {
	btr = (hr == E_OUTOFMEMORY) ? BT_OUT_OF_MEMORY : BT_INTERNAL_ERROR;
	goto exit;
  }
  new(newpage)BtBasePage(INDEX_PAGE, pageSize, m_Btree);

  if (newpage->m_PageSize < m_Btree->m_MinPageSize)
  {
	newpage->m_PageStatus.m_Status.m_PageState = MERGE_PAGE;
  }
 

  char* curSep = nullptr;
  UINT curSepLen = 0;
  KeyPtrPair* pre = nullptr;

  UINT tpos = 0;
  for (UINT spos = 0; spos < m_nSortedSet; spos++)
  {
	if (spos != dropPos)
	{
	  pre = GetKeyPtrPair(spos);
	  curSep = (char*)(this) + pre->m_KeyOffset;
	  newpage->AppendToSortedSet(curSep, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
	}
	tpos++;
  }

  UINT frontSize = UINT((char*)(&newpage->m_RecordArr[m_nSortedSet-1]) - (char*)(newpage));
  UINT backSize = newpage->KeySpaceSize();
  _ASSERTE(frontSize + backSize == pageSize);

exit:
  newIndexPage = newpage;
  return btr;

}


void BtBasePage::DeletePage(BtIterator* iter)
{
  // Check the index pages on the path up to the root looking for the first page that
  // conatins more then one record entry. This page will be updated and all index pages 
  // below it will be deleted because they will become empty. 
  const UINT MAXPAGES2DELETE = 10;
  BtBasePage* pages2Delete[MAXPAGES2DELETE];
  UINT deleteCount = 0;

  BtBasePage* parentPage = nullptr;
  int parentIndx = 0;
  for (parentIndx = iter->m_Count - 2; parentIndx >= 0; parentIndx--)
  {
	parentPage = (BtBasePage*)(iter->m_Path[parentIndx].m_Page);
	if (parentPage->LiveRecordCount() > 1)
	{
	  break;
	}
	pages2Delete[deleteCount] = parentPage;
	deleteCount++;
  }

  // Create a new instance of the parent page without the separator and pointer
  // for the current page. Then update the pointer in the grandparent page or b-tree object
  UINT dropPos = iter->m_Path[parentIndx].m_Slot;
  BtBasePage* newIndxPage = nullptr;
  BTRESULT hr = BT_SUCCESS;
  if (parentPage)
  {
	parentPage->ShrinkIndexPage(dropPos, newIndxPage);
  }
 
  // Determine where to install the new page
  BtBasePage** installAddr = nullptr;
  if (parentIndx == -1)
  {
	// Deleting the last page of the tree so set then root pointer to null.
	_ASSERTE(newIndxPage == nullptr);
	installAddr = const_cast<BtBasePage**>(&m_Btree->m_RootPage);
  }  else
  if (parentIndx == 0)
  {
	// The b-tree object is the grandparent
	installAddr = const_cast<BtBasePage**>(&m_Btree->m_RootPage);
  }
  else
  {
	BtBasePage* grandParentPage = (BtBasePage*)(iter->m_Path[parentIndx-1].m_Page);
	KeyPtrPair* recEntry = grandParentPage->GetKeyPtrPair(iter->m_Path[parentIndx-1].m_Slot);
	installAddr = (BtBasePage**)(&recEntry->m_RecPtr);
  }
  *installAddr = newIndxPage;
  // TODO Make installation atomic...

  // Delete index pages that are no longer needed.
  for (UINT i = 0; i < deleteCount; i++)
  {
	BtBasePage* pg = pages2Delete[i];
	m_Btree->m_EpochMgr->Deallocate(pg, MemObjectType::IndexPage);
    m_Btree->m_nIndexPages--;
  }
  // Finally delete the current leaf page
  m_Btree->m_nLeafPages--;
  m_Btree->m_EpochMgr->Deallocate(this, MemObjectType::IndexPage);


}

BTRESULT BtreeRootInternal::InstallNewPages(BtIterator* iter, BtBasePage* leftPage, BtBasePage* rightPage, char* separator, UINT sepLen)
{
  UINT addedIndexPages = 0;
  BtBasePage* newParentPage = nullptr;
  BtBasePage* parentPage = nullptr;
  BtBasePage* expVal = nullptr;
  BtBasePage** installAddr = nullptr;
  BTRESULT btr = BT_SUCCESS;

  // Install the two new pages
    if (iter->m_Count == 1)
    {
	// B-tree object is the parent so update there
	newParentPage = CreateIndexPage(leftPage, rightPage, separator, sepLen);
	expVal = const_cast<BtBasePage*>(m_RootPage);
	installAddr = const_cast<BtBasePage**>(&(m_RootPage));
	addedIndexPages++;
   }
   else
    {
        // Create a new instance of the parent index page that includes the new separator 
        // and the two new pages. Then update the pointer in the grandparent page.
        parentPage = (BtBasePage*)(iter->m_Path[iter->m_Count - 2].m_Page);
            UINT oldPos = iter->m_Path[iter->m_Count - 2].m_Slot;

        HRESULT hr = parentPage->ExpandIndexPage(separator, sepLen, leftPage, rightPage, oldPos, newParentPage);

        if (iter->m_Count == 2)
        {
            // The b-tree object is the grandparent
	        installAddr = const_cast<BtBasePage**>(&m_RootPage);
 
        }
        else
        {
            BtBasePage* grandParentPage = (BtBasePage*)(iter->m_Path[iter->m_Count - 3].m_Page);
            KeyPtrPair* recEntry = grandParentPage->GetKeyPtrPair(iter->m_Path[iter->m_Count - 3].m_Slot);
            installAddr = (BtBasePage**)(&recEntry->m_RecPtr);
        }
	    expVal = parentPage;
    }

  LONG64 oldVal = InterlockedCompareExchange64((LONG64*)(installAddr), LONG64(newParentPage), LONG64(expVal));
  if (oldVal == LONG64(expVal))
  {
	// Success so delete the old parent page (if there was one)
	if( parentPage) m_EpochMgr->Deallocate(parentPage, MemObjectType::IndexPage);
	m_nIndexPages += addedIndexPages;
  }
  else
  {
	// Failure so delete the new index page
	m_EpochMgr->DeallocateNow(newParentPage, MemObjectType::IndexPage);
	btr = BT_INSTALL_FAILED;
  }

    return btr;
}

BTRESULT BtreeRoot::InsertRecord(KeyType* key, void* recptr)
{
    if (key == nullptr || key->m_pKeyValue == nullptr || key->m_KeyLen == 0 || recptr == nullptr)
    {
        return BT_INVALID_ARG;
    }
    BtreeRootInternal* btreeInt = (BtreeRootInternal*)(this);
    BTRESULT br = btreeInt->InsertRecordInternal(key, recptr);
    btreeInt->CheckTree(stdout);
    return br;
}

BTRESULT BtreeRoot::LookupRecord(KeyType* key, void*& recFound)
{
    if (key == nullptr || key->m_pKeyValue == nullptr || key->m_KeyLen == 0 )
    {
        return BT_INVALID_ARG;
    }
    BtreeRootInternal* btreeInt = (BtreeRootInternal*)(this);
    return btreeInt->LookupRecordInternal(key, recFound);
}

BTRESULT BtreeRoot::DeleteRecord(KeyType* key)
{
    if (key == nullptr || key->m_pKeyValue == nullptr || key->m_KeyLen == 0 )
    {
        return BT_INVALID_ARG;
    }
    BtreeRootInternal* btreeInt = (BtreeRootInternal*)(this);
    return btreeInt->DeleteRecordInternal(key);
}

void BtreeRoot::PrintStats(FILE* file)
{
  BtreeRootInternal* root = (BtreeRootInternal*)(this);
  root->PrintTreeStats(file);
}

void BtreeRoot::CheckTree(FILE* fh)
{
	BtreeRootInternal* btreeInt = (BtreeRootInternal*)(this);
    return btreeInt->CheckTree(fh);
}

BtreeRootInternal::BtreeRootInternal()
{
  m_MemoryBroker = new MemoryBroker(m_MemoryAllocator);
  m_EpochMgr = new EpochManager();
  m_EpochMgr->Initialize(m_MemoryBroker, this, nullptr);
  m_RootPage = nullptr;
  m_nInserts = m_nDeletes = m_nUpdates = 0;
  m_nRecords = m_nLeafPages = m_nIndexPages = 0;
  m_nPageSplits = m_nConsolidations = m_nPageMerges = 0;
}

// Compute the page size to allocate
UINT BtreeRootInternal::ComputeLeafPageSize(UINT nrRecords, UINT keySpace, UINT minFree)
{
  UINT frontSpace = BtLeafPage::LeafPageHeaderSize();
  UINT minKeySpace = nrRecords * sizeof(KeyPtrPair) + keySpace;
  UINT freeSpace = max(minFree, UINT(minKeySpace*m_FreeSpaceFraction));

  UINT pageSize = frontSpace + minKeySpace + freeSpace;
  return pageSize;
}

UINT BtreeRootInternal::ComputeIndexPageSize(UINT fanout, UINT keySpace)
{
  UINT frontSize = BtBasePage::PageHeaderSize();
  INT  pageSize = frontSize + fanout * sizeof(KeyPtrPair) + keySpace;
  return pageSize;
}


// Find the path down to the target leaf page and store it in iter.
// The function does not include closed or closing pages in the path.
// However, when accessing the pages later on, their status may have changed.
//
BTRESULT BtreeRootInternal::FindTargetPage(KeyType* searchKey, BtIterator* iter)
{
tryagain:
     BTRESULT hr = BT_SUCCESS;
     iter->m_Count = 0;
     PageStatusUnion  pst;

    // Descend down to the correct leaf page
    BtBasePage* curPage = const_cast<BtBasePage*>(m_RootPage);
    while (curPage && curPage->IsIndexPage())
    {
        BtBasePage* indxPage = (BtBasePage*)(curPage);
        pst.m_Status64 = indxPage->m_PageStatus.m_Status64;
       
        // Can't trust a closed or closing page
        if (pst.m_Status.m_PageState == PAGE_CLOSED || pst.m_Status.m_PageState == PAGE_CLOSING)
        {
            goto tryagain;
        }

        int slot = indxPage->KeySearchGE(searchKey );
        _ASSERTE(slot >= 0 && UINT(slot) < indxPage->m_nSortedSet);
        iter->ExtendPath(curPage, slot);
 

        // Is it time to split this index page?
        if (indxPage->m_PageStatus.m_Status.m_PageState == SPLIT_PAGE)
        {
            BTRESULT hr = indxPage->SplitIndexPage(iter );
            if (hr == BT_SUCCESS)
            {
                m_nPageSplits++;
            }
            goto tryagain;
        } else
		if (indxPage->m_PageStatus.m_Status.m_PageState == MERGE_PAGE && iter->m_Count > 1)
		{
		  // Try to merge this page with its left or right neighbour
		  BTRESULT hr = indxPage->TryToMergeIndexPage(iter);
		  if (hr == BT_SUCCESS)
		  {
			goto tryagain;
        }
		}
 
        curPage = (BtBasePage*)(indxPage->GetKeyPtrPair(slot)->m_RecPtr);
    }

    if (curPage)
    {
         // Can't trust a closed or closing page
       pst.m_Status64 = curPage->m_PageStatus.m_Status64;
       if (pst.m_Status.m_PageState == PAGE_CLOSED || pst.m_Status.m_PageState == PAGE_CLOSING)
        {
            goto tryagain;
        }
        _ASSERTE(curPage->IsLeafPage() && curPage->m_PageStatus.m_Status.m_PageState == PAGE_NORMAL);
        iter->ExtendPath(curPage, -1);
    }
    return hr;
}


BTRESULT BtreeRootInternal::InsertRecordInternal(KeyType* key, void* recptr)
{
    LONGLONG epochId = 0;
    m_EpochMgr->EnterEpoch(&epochId);

tryagain:
     BTRESULT btr = BT_SUCCESS;
     BtIterator  iter(this);
 
    BtBasePage* rootbase = const_cast<BtBasePage*>(m_RootPage);

    // Create a new root page if there isn't one already
    while (rootbase == nullptr)
    {
        BtBasePage* page = CreateEmptyLeafPage();
        if (!page)
        {
            btr = BT_OUT_OF_MEMORY;
            goto exit;
        }
        LONG64 oldval = InterlockedCompareExchange64((LONG64*)(&m_RootPage), LONG64(page), LONG64(0));
        if (oldval != LONG64(0))
        {
            // Another thread already created the page so delete our version
            m_EpochMgr->DeallocateNow(page, MemObjectType::LeafPage);
        }
        rootbase = const_cast<BtBasePage*>(m_RootPage);
		m_nLeafPages++;
    }


    // Locate the target leaf page for the insertion
    btr = FindTargetPage(key, &iter);
    BtLeafPage* leafPage = (BtLeafPage*)(iter.m_Path[iter.m_Count-1].m_Page);
    _ASSERTE(leafPage && btr == BT_SUCCESS);

     btr = leafPage->AddRecordToPage(key, recptr);
     if (btr == BT_SUCCESS) 
     {
         m_nRecords++;
		 m_nInserts++;
         goto exit; 
     }

    // Page is full so either enlarge and consolidate it or split it
    // Try consolidation first 
     BtLeafPage* newPage = nullptr;
     btr = leafPage->ConsolidateLeafPage(&iter, key->m_KeyLen+sizeof(KeyPtrPair) );
     if (btr == BT_SUCCESS)
     {
        goto tryagain;
     }

     // Consolidation failed so try to split the page instead
    btr = leafPage->SplitLeafPage(&iter);
    if (btr == BT_SUCCESS)
    {
        m_EpochMgr->Deallocate(leafPage, MemObjectType::LeafPage);
        m_nPageSplits++;
        goto tryagain;   
    }

exit:
    m_EpochMgr->ExitEpoch(epochId);
    return btr;
}

BTRESULT BtreeRootInternal::DeleteRecordInternal(KeyType* key)
{
      LONGLONG epochId = 0;
    m_EpochMgr->EnterEpoch(&epochId);

    BTRESULT btr = BT_SUCCESS;
    BtIterator  iter(this);

    // Locate the target leaf page 
    btr = FindTargetPage(key, &iter);
    BtLeafPage* leafPage = (BtLeafPage*)(iter.m_Path[iter.m_Count - 1].m_Page);
    _ASSERTE(leafPage && btr == BT_SUCCESS);

    btr = leafPage->DeleteRecordFromPage(key);
    if (btr == BT_SUCCESS)
    {
	InterlockedDecrement(&m_nRecords);
	m_nDeletes++;

	if (leafPage->LiveRecordCount() == 0)
        {
	  leafPage->DeletePage(&iter);
	}
	else
	  if (leafPage->m_PageSize > m_MinPageSize && leafPage->m_WastedSpace > leafPage->NetPageSize()*m_FreeSpaceFraction)
            {
		// Try to consolidate the page
		btr = leafPage->ConsolidateLeafPage(&iter, 0);

            }
	  else
        if (leafPage->m_PageSize <= m_MinPageSize)
        {
            //Try to merge page with left or right neighbor
		  leafPage->TryToMergeLeafPage(&iter);
        }
	// leafPage may have been deleted so no more references to it after this
        }

    m_EpochMgr->ExitEpoch(epochId);
    return btr;
}


BTRESULT BtreeRootInternal::LookupRecordInternal(KeyType* key, void*& recFound)
{
    BTRESULT btr = BT_SUCCESS;
    BtIterator iter(this);
    recFound = nullptr;

    LONGLONG epochId = 0;
    m_EpochMgr->EnterEpoch(&epochId);

    // Locate the target leaf page
    btr = FindTargetPage(key, &iter);
    if (btr != BT_SUCCESS)
    {
        goto exit;
    }
    BtBasePage* leafPage = (BtBasePage*)(iter.m_Path[iter.m_Count - 1].m_Page);
    _ASSERTE(leafPage);
    if (!leafPage)
    {
        btr = BT_INTERNAL_ERROR;
        goto exit;
    }

    // Found the target leaf page, now look for the record
    int pos = leafPage->KeySearchGE(key);
    if (pos < 0)
    {
        btr = BT_KEY_NOT_FOUND;
        goto exit;
    }

    KeyPtrPair* kpp = leafPage->GetKeyPtrPair(pos);
    _ASSERTE(kpp);
    recFound = const_cast<void*>(kpp->m_RecPtr);

exit:
    m_EpochMgr->ExitEpoch(epochId);
    return btr;
}

void BtreeRootInternal::Print(FILE* file)
{
  BtBasePage* rootPage = const_cast<BtBasePage*>(m_RootPage);
  if (rootPage)
  {
      rootPage->PrintPage(file, 1);
  }
  else
  {
	fprintf(file, "B-tree is empty\n");
  }
}

void BtreeRootInternal::ComputeTreeStats(BtreeStatistics* statsp)
{
  BtBasePage* rootPage = const_cast<BtBasePage*>(m_RootPage);
  if (rootPage)
  {
	if (rootPage->IsIndexPage())
	{
	  BtBasePage* ip = (BtBasePage*)(rootPage);
	  ip->ComputeTreeStats(statsp);
	}
	else
	{
	  BtLeafPage* lp = (BtLeafPage*)(rootPage);
	  lp->ComputeTreeStats(statsp);
	}
  }
}

void BtreeRootInternal::PrintTreeStats(FILE* file)
{
  BtreeStatistics stats;
  stats.Clear();
  ComputeTreeStats(&stats);

  fprintf(file, "\n=========== B-tree statistics ===============\n");
  fprintf(file, "Size: %d records, %d leaf pages, %d index pages\n",
				m_nRecords, m_nLeafPages, m_nIndexPages);
  fprintf(file, "Operations: %d inserts, %d deletes\n", m_nInserts, m_nDeletes);
  fprintf(file, "Page ops: %d consolidations, %d splits, %d merges\n", m_nConsolidations, m_nPageSplits, m_nPageMerges);

  fprintf(file, "Index pages\n");
  fprintf(file, "   Space: %d alloced, %d pages\n", stats.m_AllocedSpaceIP, stats.m_SpaceIP );
  fprintf(file, "   Space usage: %d headers, %d rec arrays, %d keys\n", stats.m_HeaderSpaceIP, stats.m_RecArrSpaceIP, stats.m_KeySpaceIP);

  fprintf(file, "Leaf pages\n");
  fprintf(file, "   Space: %d alloced, %d pages\n", stats.m_AllocedSpaceLP, stats.m_SpaceLP);
  fprintf(file, "   Space usage: %d headers, %d rec arrays, %d keys, %d free, %d deleted\n", 
	                stats.m_HeaderSpaceLP, stats.m_RecArrSpaceLP, stats.m_KeySpaceLP,
	                stats.m_FreeSpaceLP, stats.m_DeletedSpaceLP);

  fprintf(file, "=============================================\n");
}

void BtreeRootInternal::CheckTree(FILE* file)
{
    //fprintf(file, " ====== Checking tree =======\n");
    KeyType lowbound;
    KeyType hibound;

    KeyType::GetMinValue(lowbound.m_pKeyValue, lowbound.m_KeyLen);
    KeyType::GetMaxValue(hibound.m_pKeyValue, hibound.m_KeyLen);

    BtBasePage* rootPage = const_cast<BtBasePage*>(m_RootPage);
    UINT errorCount = 0;
    if (rootPage)
    {
        errorCount += rootPage->CheckPage(file, &lowbound, &hibound);
	}

    if (errorCount > 0)
    {
        fprintf(file, " ==== Tree check found %d errors ======\n", errorCount);
    }
}


BTRESULT BtLeafPage::AddRecordToPage(KeyType* key, void* recptr)
{
tryagain:

  PageStatusUnion pst;
  pst.m_Status64 = m_PageStatus.m_Status64;

  BTRESULT btr = BT_SUCCESS;
  PageStatusUnion newState;
  if (pst.m_Status.m_PageState == PAGE_CLOSING || pst.m_Status.m_PageState == PAGE_CLOSED)
  {
      btr = BT_PAGE_CLOSED;
      goto exit;
  }

  _ASSERTE(pst.m_Status.m_PageState == PAGE_NORMAL);
  if (!EnoughFreeSpace(key->m_KeyLen, pst.m_Status64))
  {
	btr = BT_PAGE_FULL;
	goto exit;  
  }
    // Reserve space for the new record
    newState.m_Status64 = pst.m_Status64;
    newState.m_Status.m_nUnsortedReserved++;
    newState.m_Status.m_LastFreeByte -= key->m_KeyLen;
    LONG64 oldval = InterlockedCompareExchange64((LONG64*)(&m_PageStatus.m_Status64), newState.m_Status64, pst.m_Status64);
    if (oldval != pst.m_Status64)
    {
	    goto tryagain;
    }
 
 
  // First copy the key into its reserved space
  char* keyBuffer = (char*)(this) + newState.m_Status.m_LastFreeByte + 1;
  memcpy(keyBuffer, key->m_pKeyValue, key->m_KeyLen);

  // Then fill in the record slot
  UINT32 slotIndx = newState.m_Status.m_nUnsortedReserved - 1;
  KeyPtrPair* pentry = GetUnsortedEntry(slotIndx); 
  pentry->m_KeyOffset = newState.m_Status.m_LastFreeByte + 1;
  pentry->m_KeyLen = key->m_KeyLen;
  MemoryBarrier();

  // Setting the record pointer makes the slot and record visible
  // Verify that the page is still in NORMAL state. 
  btr = BT_NOT_INSERTED;
  if (m_PageStatus.m_Status.m_PageState == PAGE_NORMAL)
  {
    ULONGLONG resVal = InterlockedCompareExchange64((LONGLONG*)(&pentry->m_RecPtr), ULONGLONG(recptr), 0);
    if (resVal == 0)
    {
        btr = BT_SUCCESS;
    }
  }
 
exit:
  return btr;

}

BTRESULT BtLeafPage::DeleteRecordFromPage(KeyType* key )
{

    PageStatusUnion pst;
    pst.m_Status64 = m_PageStatus.m_Status64;

    BTRESULT btr = BT_SUCCESS;
    if (pst.m_Status.m_PageState == PAGE_CLOSED || pst.m_Status.m_PageState == PAGE_CLOSING)
    {
        btr = BT_PAGE_CLOSED;
        goto exit;
    }


    PageStatusUnion newState;

    // Locate the record
    int pos = KeySearchGE(key);
    if (pos < 0)
    {
        btr = BT_KEY_NOT_FOUND;
        goto exit;
    }

    KeyPtrPair* kpp = GetKeyPtrPair(pos);
    _ASSERTE(kpp);
    ULONGLONG recPtr = 0;
    if (kpp)
    {
        recPtr = ULONGLONG(const_cast<void*>(kpp->m_RecPtr));
        ULONGLONG retVal = 0;
        if (!KeyPtrPair::IsClosedBitOn(recPtr))
        {
            retVal = InterlockedCompareExchange64((LONGLONG*)(&kpp->m_RecPtr), 0, recPtr);
            if (retVal != recPtr)
            {
                if (KeyPtrPair::IsClosedBitOn(retVal))
                {
                    // Somebody snuck in and closed the entry under us
                    btr = BT_PAGE_CLOSED;
                } else
                if( retVal == 0 )
                {
                    // Somebody snuck in and deleted the record from under us
                    btr = BT_KEY_NOT_FOUND;
                }
            } else
            {
                // Success
                btr = BT_SUCCESS;
            }
        }
        else
        {
            // Slot is closed
            btr = BT_PAGE_CLOSED;
        }
    }

    // Update page 
    if (btr == BT_SUCCESS)
    {
        // Update page status, that is, increment count of cleared slots
        while(true)
        {
            pst.m_Status64 = m_PageStatus.m_Status64;
            newState.m_Status64 = pst.m_Status64;
            PageStatusUnion retVal;
            if (pst.m_Status.m_PageState != PAGE_NORMAL)
            {
                // No need to update the page status
                break;
            }
            newState.m_Status64 = pst.m_Status64;
            newState.m_Status.m_SlotsCleared++;
            retVal.m_Status64 = InterlockedCompareExchange64(&m_PageStatus.m_Status64, newState.m_Status64, pst.m_Status64);
            if (retVal.m_Status64 == pst.m_Status64)
            {
                // Success
                InterlockedAdd(&m_WastedSpace, sizeof(KeyPtrPair) + kpp->m_KeyLen);
                break;
            }

       }
    }
 

exit:
  return btr;

}


bool BtLeafPage::EnoughFreeSpace(UINT32 keylen, UINT64 pageState)
{
  PageStatusUnion pst;
  pst.m_Status64 = pageState;

  // First free byte (from beginning of the page
  UINT  slotsUsed = m_nSortedSet + pst.m_Status.m_nUnsortedReserved;
  char* firstFree = (char*)(&m_RecordArr[slotsUsed]);

  char* lastFree = (char*)(this) + pst.m_Status.m_LastFreeByte;

  INT64 freeSpace = lastFree - firstFree + 1;
  _ASSERTE(freeSpace >= 0);

  INT64 reqSpace = sizeof(KeyPtrPair) + keylen;

  return reqSpace <= freeSpace;
}

int _cdecl QSortCompareKeys(void* context, const void* leftp, const void* rightp)
{
  KeyPtrPair* lre = (KeyPtrPair*)(leftp);
  KeyPtrPair* rre = (KeyPtrPair*)(rightp);
  BtLeafPage* page = (BtLeafPage*)(context);

  char* leftkey  = (char*)(page) + lre->m_KeyOffset;
  char* rightkey = (char*)(page) + rre->m_KeyOffset;

  return page->GetBtreeRoot()->m_CompareFn(leftkey, lre->m_KeyLen, rightkey, rre->m_KeyLen);
}

int _cdecl PermArrayCompareKeys(void* context, const void* leftp, const void* rightp)
{
    PermutationArray*  parr = (PermutationArray*)(context);
    UINT16 lidx = *(UINT16*)(leftp);
    UINT16 ridx = *(UINT16*)(rightp);
    KeyPtrPair* lkpp = parr->m_TargetPage->GetKeyPtrPair(lidx); 
    KeyPtrPair* rkpp = parr->m_TargetPage->GetKeyPtrPair(ridx); 

    char* leftkey = (char*)(parr->m_TargetPage) + lkpp->m_KeyOffset;
    char* rightkey = (char*)(parr->m_TargetPage) + rkpp->m_KeyOffset;
  
    return parr->m_TargetPage->GetBtreeRoot()->m_CompareFn(leftkey, lkpp->m_KeyLen, rightkey, rkpp->m_KeyLen);
}

BTRESULT PermutationArray::SortPermArray()
{
    qsort_s(&m_PermArray[0], m_nrEntries, sizeof(UINT16), PermArrayCompareKeys, this);
    return (errno != EINVAL) ? BT_SUCCESS : BT_INTERNAL_ERROR;
}


BTRESULT BtLeafPage::SortUnsortedSet(KeyPtrPair* pSortedArr, UINT count)
{
  qsort_s(pSortedArr, count, sizeof(KeyPtrPair), QSortCompareKeys, this);
  return (errno != 0) ? BT_INTERNAL_ERROR : BT_SUCCESS;
}

// Close the page and all its outgoing record pointers.
// Guaranteed to be true when the function exits
void BtLeafPage::ClosePage()
{
    PageStatusUnion pst;

    while (true)
    {
        pst.m_Status64 = m_PageStatus.m_Status64;

        if (pst.m_Status.m_PageState == PAGE_CLOSED)
        {
            return;
        }

        PageStatusUnion newst;
        newst.m_Status64 = pst.m_Status64;
        if (pst.m_Status.m_PageState != PAGE_CLOSING)
        {
            newst.m_Status.m_PageState = PAGE_CLOSING;
            LONG64 oldval = InterlockedCompareExchange64(&m_PageStatus.m_Status64, newst.m_Status64, pst.m_Status64);
        }

        for (UINT i = 0; i < UINT(m_nSortedSet + pst.m_Status.m_nUnsortedReserved); i++)
        {
            KeyPtrPair* kpp = GetKeyPtrPair(i);
            kpp->CloseEntry(); 
        }
        newst.m_Status.m_PageState = PAGE_CLOSED;
        LONG64 oldval = InterlockedCompareExchange64(&m_PageStatus.m_Status64, newst.m_Status64, pst.m_Status64);
    }

}

// Get a consistent count of the number of live records and the key space they require
void BtBasePage::LiveRecordSpace(UINT& recCount, UINT& keySpace)
{
  PageStatusUnion pst;

  do
  {
    pst.m_Status64 = m_PageStatus.m_Status64;

	if (IsIndexPage())
	{
	  recCount = m_nSortedSet;
	  keySpace = KeySpaceSize();
	}
	else
	{

	  BtLeafPage* page = (BtLeafPage*)(this);

    // Count the number of records remaining on the page and the amount of space needed for the new page
	  recCount = 0;
	  keySpace = 0;
      KeyPtrPair* pre = nullptr;
	for (UINT i = 0; i < m_nSortedSet + pst.m_Status.m_nUnsortedReserved ; i++)
    {
		pre = page->GetKeyPtrPair(i);
        if (pre->m_RecPtr)
        {
            recCount++;
            keySpace += pre->m_KeyLen;
        }
    }

	}
	
	// Try again if page status has changed. 
  } while (pst.m_Status64 != m_PageStatus.m_Status64);

}

BTRESULT BtLeafPage::ExtractLiveRecords(KeyPtrPair*& liveRecArray, UINT& count, UINT& keySpace)
    {
  _ASSERTE(IsClosed());
  BTRESULT btr = BT_SUCCESS;

  // Allocate array for final result
  UINT liveRecs = LiveRecordCount();
  liveRecArray = nullptr;
  count = 0;
  keySpace = 0;
  HRESULT hr = m_Btree->m_MemoryBroker->Allocate(liveRecs*sizeof(KeyPtrPair), (void**)(&liveRecArray), MemObjectType::TmpPointerArray);
  if (hr != S_OK)
        {
	btr = BT_OUT_OF_MEMORY;
	goto exit;
  }
  // Copy in the the live records from the sorted set
  UINT recCount = 0;
  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pre = &m_RecordArr[i];
	if (!pre->IsDeleted())
	{
	  liveRecArray[recCount] = *pre;
            recCount++;
            keySpace += pre->m_KeyLen;
        }
    }

  // Copy the unsorted record entries into an array and sort them
  //
  KeyPtrPair* sortArr = nullptr;
  UINT nrUnsorted = m_PageStatus.m_Status.m_nUnsortedReserved;
  UINT srcCount = 0;
  if (nrUnsorted > 0)
    {
	HRESULT hr = m_Btree->m_MemoryBroker->Allocate(nrUnsorted * sizeof(KeyPtrPair), (void**)(&sortArr), MemObjectType::TmpPointerArray);
	if (hr != S_OK)
	{
	  btr = BT_OUT_OF_MEMORY;
        goto exit;
    }

	KeyPtrPair* pre = nullptr;
	for (UINT i = 0; i < nrUnsorted; i++)
    {
	  pre = GetUnsortedEntry(i);
	  if (pre->m_RecPtr != nullptr)
	  {
		sortArr[srcCount] = *pre;
		srcCount++;
    }
	}
	btr = SortUnsortedSet(sortArr, srcCount);
  }
 
  // Now merge the newly sorted records into liveRecArray.
  // The numberof records in sortArr is typically much smaller
  // then the number of records in liveRecArray.

  if (srcCount > 0)
    {
	int trgtIndx = recCount + srcCount -1;
	int liveIndx = recCount - 1;
	int recsCopied = 0;
	for (int srcIndx = srcCount - 1; srcIndx >= 0; srcIndx--)
	{
	  char* srcKey = (char*)(this) + sortArr[srcIndx].m_KeyOffset;
	  UINT  srcKeyLen = sortArr[srcIndx].m_KeyLen;

	  // Advance until we find a record with a key less than or equal to the srcKey
	  for (; liveIndx >= 0; liveIndx--)
	  {
		char* liveKey = (char*)(this) + liveRecArray[liveIndx].m_KeyOffset;
		UINT liveKeyLen = liveRecArray[liveIndx].m_KeyLen;
		
		// Copy larger key into the target position
		recsCopied++;
		int cv = m_Btree->m_CompareFn(liveKey, liveKeyLen, srcKey, srcKeyLen);
		if (cv <= 0)
		{
		  // Copy from srcIndx into target position
		  liveRecArray[trgtIndx] = sortArr[srcIndx];
		  trgtIndx++;
		  break;
    }
		// Copy  from liveIndx into target poisition
		liveRecArray[trgtIndx] = liveRecArray[liveIndx];
		trgtIndx++;
	  }
	}
  }
  count = recCount + srcCount;

  for (UINT i = 0; i < count; i++)
  {
	KeyPtrPair* pre = &liveRecArray[i];
	pre->CleanEntry();
	_ASSERTE(!pre->IsDeleted());
  }

exit:
  if (btr != BT_SUCCESS)
  {
	if (liveRecArray)
	{
	  m_Btree->m_MemoryBroker->DeallocateNow(liveRecArray, MemObjectType::TmpPointerArray);
	  liveRecArray = nullptr;
	  count = 0;
	}
  }
  return btr;
}

BTRESULT BtLeafPage::CopyToNewPage(BtLeafPage* newPage)
{
  PageStatusUnion pst;
  pst.m_Status64 = m_PageStatus.m_Status64;

  KeyPtrPair* sortArr = nullptr;
  BTRESULT btr = BT_SUCCESS;
  
    // Copy the unsorted record entries into an array and sort them
  //
  UINT nrUnsorted = pst.m_Status.m_nUnsortedReserved;
  UINT rCount = 0; 
  if (nrUnsorted > 0)
  {
	HRESULT hr = m_Btree->m_MemoryBroker->Allocate(nrUnsorted * sizeof(KeyPtrPair), (void**)(&sortArr), MemObjectType::TmpPointerArray);
    if (hr != S_OK)
    {
        btr = BT_OUT_OF_MEMORY;
        goto exit;
    }


	KeyPtrPair* pre = nullptr;
    for (UINT i = 0; i < nrUnsorted; i++)
    {
        pre = GetUnsortedEntry(i);
	  if (pre->m_RecPtr != nullptr)
	  {
		sortArr[rCount] = *pre;
		rCount++;
    }
	}
	btr = SortUnsortedSet(sortArr, rCount);
  }

    // Merge the record entries from the sorted set on the input page
    // and the record entries from the sort array to create the sorted set of the new page
    KeyPtrPair* pLeft = GetKeyPtrPair(0);
  KeyPtrPair* pRight = (KeyPtrPair*)(sortArr);
    INT lCount = m_nSortedSet;

  UINT copiedRecs = 0;
  UINT copiedSpace = 0;


    while (lCount > 0 && rCount > 0)
    {
        // Skip deleted entries in the sorted set
        if (pLeft->m_RecPtr == nullptr)
        {
            pLeft++;
            lCount--;
            continue;
        }
        // Should we pull from left or right input to the merge
        char* lKey = (char*)(this) + pLeft->m_KeyOffset;
        char* rKey = (char*)(this) + pRight->m_KeyOffset;
        int cv = m_Btree->m_CompareFn(lKey, pLeft->m_KeyLen, rKey, pRight->m_KeyLen);

        if (cv <= 0)
        {
            // Copy record entry and key value from left input
	  newPage->AppendToSortedSet(lKey, pLeft->m_KeyLen, const_cast<void*>(pLeft->m_RecPtr));
	  copiedRecs++;
	  copiedSpace += pLeft->m_KeyLen;

	  // Advance left input
            pLeft++;
            lCount--;
        }
        else
        {
            // Copy record entry and key value from left input
	  newPage->AppendToSortedSet(rKey, pRight->m_KeyLen, const_cast<void*>(pRight->m_RecPtr));

	  copiedRecs++;
	  copiedSpace += pRight->m_KeyLen;

	  // Advance right input
            pRight++;
            rCount--;
        }
    }

    // Copy remaining, if any, from left or right
    while (lCount > 0)
    {
	if (pLeft->m_RecPtr == nullptr)
	{
	  pLeft++;
	  lCount--;
	  continue;
	}

        // Copy record entry and key value from left input
        char* lKey = (char*)(this) + pLeft->m_KeyOffset;
	newPage->AppendToSortedSet(lKey, pLeft->m_KeyLen, const_cast<void*>(pLeft->m_RecPtr));
	copiedRecs++;
	copiedSpace += pLeft->m_KeyLen;

	// Advance left input
        pLeft++;
        lCount--;
    }

    while (rCount > 0)
    {
        // Copy record entry and key value from right input
        char* rKey = (char*)(this) + pRight->m_KeyOffset;
	newPage->AppendToSortedSet(rKey, pRight->m_KeyLen, const_cast<void*>(pRight->m_RecPtr));
	copiedRecs++;
	copiedSpace += pRight->m_KeyLen;

	// Advance right input
        pRight++;
        rCount--;
    }

    for (UINT i = 0; i < newPage->m_nSortedSet; i++)
    {
        newPage->m_RecordArr[i].CleanEntry();
    }

exit:
  return btr;
}

BTRESULT BtLeafPage::ConsolidateLeafPage(BtIterator* iter, UINT minFree)
{

    // Get a stable copy of the page status
    PageStatusUnion pst;
    pst.m_Status64 = m_PageStatus.m_Status64;

    BTRESULT btr = BT_SUCCESS;
    BtLeafPage* newPage = nullptr;


    UINT recCount = 0;
    UINT keySpace = 0;	
	LiveRecordSpace(recCount, keySpace);

    UINT newSize = m_Btree->ComputeLeafPageSize(recCount, keySpace, minFree);

    // Don't expand beyond maximum page size
    if (newSize > m_Btree->m_MaxPageSize)
    {
        btr = BT_MAX_SIZE;
        goto exit;
    }

    if (pst.m_Status64 != m_PageStatus.m_Status64)
    {
        goto exit;
    }
    ClosePage();
    _ASSERTE(m_PageStatus.m_Status.m_PageState == PAGE_CLOSED);
 

    // Allocate the new page
    HRESULT hr = m_Btree->m_MemoryBroker->Allocate(newSize, (void**)(&newPage), MemObjectType::LeafPage);
    if (hr != S_OK)
    {
        btr = BT_OUT_OF_MEMORY;
        goto exit;
    }
    new(newPage) BtLeafPage(newSize, m_Btree);

	btr = CopyToNewPage(newPage);

	_ASSERTE(newPage->m_nSortedSet == recCount);
	_ASSERTE(newPage->KeySpaceSize() == keySpace);


    // Finally install the new page
    if (newPage)
    {
        BtBasePage** installAddr = nullptr;
        if (iter->m_Count == 1)
        {
            installAddr = const_cast<BtBasePage**>(&m_Btree->m_RootPage);
        }
        else
        {
            BtBasePage* parentPage = (BtBasePage*)(iter->m_Path[iter->m_Count - 2].m_Page);
            KeyPtrPair* recEntry = parentPage->GetKeyPtrPair(iter->m_Path[iter->m_Count - 2].m_Slot);
            installAddr = (BtBasePage**)(&recEntry->m_RecPtr);
        }

        LONG64 oldval = InterlockedCompareExchange64((LONG64*)(installAddr), LONG64(newPage), LONG64(this));
        if (oldval != LONG64(this))
        {
            btr = BT_INSTALL_FAILED;
        }
    }

exit:

    if (btr != BT_SUCCESS)
    {
        if (newPage) m_Btree->m_EpochMgr->Deallocate(newPage, MemObjectType::LeafPage);
        newPage = nullptr;
    }
	else
	{
	  InterlockedIncrement(&m_Btree->m_nConsolidations);
	  newPage->m_Btree->m_MemoryBroker->Free(this, MemObjectType::LeafPage);
	}
    return btr;
}


BTRESULT BtLeafPage::SplitLeafPage(BtIterator* iter)
{
  // The page is assumed to be closed at this point so its state remains unchanged

  BTRESULT btr = BT_SUCCESS;

  // Copy a record entries for reccords that have not been deleted into a temporary array for sorting
  UINT nrSlots = m_nSortedSet + m_PageStatus.m_Status.m_nUnsortedReserved;
  KeyPtrPair* sortArr = nullptr;
  HRESULT hr = m_Btree->m_MemoryBroker->Allocate(nrSlots * sizeof(KeyPtrPair), (void**)(&sortArr), MemObjectType::TmpPointerArray);
  if( hr != S_OK)
  {
      btr = BT_OUT_OF_MEMORY;
      goto exit;
  }

  UINT nrRecords = 0;
  KeyPtrPair* pre = nullptr;
  for (UINT i = 0; i < nrSlots; i++)
  {
	pre = GetKeyPtrPair(i);
	if (pre && pre->m_RecPtr)
	{
	  sortArr[nrRecords] = *pre;
	  nrRecords++;
	}
  }

  qsort_s(sortArr, nrRecords, sizeof(KeyPtrPair), QSortCompareKeys, this);
  if (errno == EINVAL) 
  { 
      btr = BT_INTERNAL_ERROR; 
      goto exit; 
  }

  UINT lCount = nrRecords / 2;
  UINT rCount = nrRecords - lCount;

  // Copy first lCount records to the left new page (lower keys)
  UINT keySpace = 0;
  for (UINT i = 0; i < lCount; i++) keySpace += GetKeyPtrPair(i)->m_KeyLen;
  UINT pageSize = m_Btree->ComputeLeafPageSize(lCount, keySpace, 0);

  BtLeafPage* leftPage = nullptr;
  hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&leftPage), MemObjectType::LeafPage);
  if (hr != S_OK) 
  { 
      btr = BT_OUT_OF_MEMORY;
      goto exit; 
  }
  new(leftPage)BtLeafPage(pageSize, m_Btree);

  for (UINT i = 0; i < lCount; i++)
  {
	char* key = (char*)(this) + sortArr[i].m_KeyOffset;
	leftPage->AppendToSortedSet(key, sortArr[i].m_KeyLen, const_cast<void*>(sortArr[i].m_RecPtr));
  }

  // Copy the higher rCount records into the right new page (higher keys)
  keySpace = 0;
  for (UINT i = lCount; i < nrRecords; i++) keySpace += GetKeyPtrPair(i)->m_KeyLen;
  pageSize = m_Btree->ComputeLeafPageSize(rCount, keySpace, 0);

  BtLeafPage* rightPage = nullptr;
  hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&rightPage), MemObjectType::LeafPage);
  if (hr != S_OK) 
  { 
      btr = BT_OUT_OF_MEMORY;
      goto exit; 
  }
  new(rightPage)BtLeafPage(pageSize, m_Btree);

  for (UINT i = lCount; i < nrRecords; i++)
  {
	char* key = (char*)(this) + sortArr[i].m_KeyOffset;
	rightPage->AppendToSortedSet( key, sortArr[i].m_KeyLen, const_cast<void*>(sortArr[i].m_RecPtr));
  }
 
  // Use the last key of the left page as separator for the two pages.
  // A separator thus indicates the highest key value allowed on a page.
  // The separator will be added to the parent index page.
  char* separator = (char*)(leftPage)+leftPage->GetKeyPtrPair(lCount-1)->m_KeyOffset;
  UINT seplen = leftPage->GetKeyPtrPair(lCount-1)->m_KeyLen;

  // Install the new pages
  btr = m_Btree->InstallNewPages(iter, leftPage, rightPage, separator, seplen);
  if (btr == BT_INSTALL_FAILED)
  {
	m_Btree->m_EpochMgr->Deallocate(leftPage, MemObjectType::LeafPage);
	m_Btree->m_EpochMgr->Deallocate(rightPage, MemObjectType::LeafPage);
  }
  else
  {
	m_Btree->m_nPageSplits++;
	m_Btree->m_nLeafPages++;
  }

#ifdef DISABLED
  fprintf(stdout, "\n=== Splitting leaf page ========= \n");
  fprintf(stdout, "  === source: ");
  ShortPrint(stdout);
  fprintf(stdout, "\n  ===   left: ");
  leftPage->ShortPrint(stdout);
  fprintf(stdout, "\n              Separator: %1.*s\n", seplen, separator);
  fprintf(stdout, "  ===  right: ");
  rightPage->ShortPrint(stdout);
  fprintf(stdout, "\n===================================\n");
#endif

 exit:
   return btr;
 }

 BTRESULT BtLeafPage::TryToMergeLeafPage(BtIterator* iter)
  {
   BTRESULT btr = BT_SUCCESS;

   BtLeafPage* newPage = nullptr;

   UINT myCount = 0, myKeySpace = 0;
   UINT leftCount = 0, leftKeySpace = 0;
   UINT rightCount = 0, rightKeySpace = 0;

   BtBasePage* parent = (BtBasePage*)(iter->m_Path[iter->m_Count - 2].m_Page);
   UINT mySlot = iter->m_Path[iter->m_Count - 2].m_Slot;

   LiveRecordSpace(myCount, myKeySpace);

   BtLeafPage* leftPage = nullptr;
   BtLeafPage* rightPage = nullptr;

   if (mySlot > 0)
   {
	 leftPage = (BtLeafPage*)(parent->GetKeyPtrPair(mySlot - 1)->m_RecPtr);
	 leftPage->LiveRecordSpace(leftCount, leftKeySpace);
  }
   if (mySlot+1 < parent->SortedSetSize())
   {
	 rightPage = (BtLeafPage*)(parent->GetKeyPtrPair(mySlot + 1)->m_RecPtr);
	 rightPage->LiveRecordSpace(rightCount, rightKeySpace);
   }

   UINT slotToDelete = 0;
   enum mergeType { NONE, LEFT, RIGHT} mergeDir;
   mergeDir = NONE;
   if (leftPage && rightPage)
   {
	 // OK to merge either way
	 mergeDir = (leftCount * sizeof(KeyPtrPair) + leftKeySpace < rightCount * sizeof(KeyPtrPair) + rightKeySpace) ? LEFT : RIGHT;
   } else
	if (leftPage)
	{
	  // OK to merge left
	  mergeDir = LEFT;
	} else 
	if (rightPage)
	{
	  // OK to merge right
	  mergeDir = RIGHT;
	}


   // Merge with the smaller neighbouring page
   if ( mergeDir == LEFT)
   {
	 // Merge with left
	 UINT pageSize = m_Btree->ComputeLeafPageSize(leftCount + myCount, leftKeySpace + myKeySpace, 0);
	 if (pageSize <= m_Btree->m_MaxPageSize)
	 {
		ClosePage();
		leftPage->ClosePage();
		btr = MergeLeafPages(leftPage, false, &newPage);
	   rightPage = nullptr;
	   slotToDelete = mySlot - 1;
	 }

   } else
   if (mergeDir == RIGHT)
	{
	  // Merge with right
	  UINT pageSize = m_Btree->ComputeLeafPageSize(rightCount + myCount, rightKeySpace + myKeySpace, 0);
	  if (pageSize <= m_Btree->m_MaxPageSize)
	  {
		ClosePage();
		rightPage->ClosePage();
		btr = MergeLeafPages(rightPage, true, &newPage);
		leftPage = nullptr;
		slotToDelete = mySlot;
	  }
   }
  else
  {
	 leftPage = nullptr;
	 rightPage = nullptr;
	 newPage = nullptr;
	 btr = BT_NO_MERGE;
   }

  BtBasePage* newParent = nullptr;
  if (newPage)
   {

	 // Create new parent index page

	 parent->ShrinkIndexPage(slotToDelete, newParent);
	 KeyPtrPair* myNewEntry = newParent->GetKeyPtrPair(slotToDelete);
	 myNewEntry->m_RecPtr = newPage;

	 // Determine where to install the new page
      BtBasePage** installAddr = nullptr;
	 int parentIndx = iter->m_Count - 2;
	 _ASSERTE(parentIndx >= 0);

	 if (parentIndx == 0)
      {
          // The b-tree object is the grandparent
          installAddr = const_cast<BtBasePage**>(&m_Btree->m_RootPage);
      }
      else
      {
	   BtBasePage* grandParentPage = (BtBasePage*)(iter->m_Path[parentIndx - 1].m_Page);
	   KeyPtrPair* recEntry = grandParentPage->GetKeyPtrPair(iter->m_Path[parentIndx - 1].m_Slot);
          installAddr = (BtBasePage**)(&recEntry->m_RecPtr);
      }

	 LONG64 oldval = InterlockedCompareExchange64((LONG64*)(installAddr), LONG64(newParent), LONG64(parent));
	 if (oldval != LONG64(parent))
	 {
	   btr = BT_INSTALL_FAILED;
	   goto exit;
  }
	 InterlockedIncrement(&m_Btree->m_nPageMerges);
  }


 exit:
   if (btr != BT_SUCCESS)
   {
	 if (newPage) m_Btree->m_MemoryBroker->DeallocateNow(newPage, MemObjectType::LeafPage);
	 newPage = nullptr;
	 if (newParent) m_Btree->m_MemoryBroker->DeallocateNow(newParent, MemObjectType::IndexPage);
	 newParent = nullptr;
   }
   else
   {
	 if (leftPage)  m_Btree->m_MemoryBroker->Free(leftPage, MemObjectType::LeafPage);
	 if (rightPage) m_Btree->m_MemoryBroker->Free(rightPage, MemObjectType::LeafPage);
	 InterlockedDecrement(&m_Btree->m_nLeafPages);
	 newPage->m_Btree->m_MemoryBroker->Free(this, MemObjectType::LeafPage);
   }
   
   return btr;

 }

 // Merge two leaf pages: this and otherPage. 
 BTRESULT BtLeafPage::MergeLeafPages(BtLeafPage* otherPage, bool mergeOnRight, BtLeafPage** newPage)
 {

   BTRESULT btr = BT_SUCCESS;

   KeyPtrPair* leftSortedArr = nullptr;
   char* leftBase = nullptr;
   UINT leftRecCount = 0;
   UINT leftKeySpace = 0;
   KeyPtrPair* rightSortedArr = nullptr;
   char* rightBase = nullptr;
   UINT rightRecCount = 0;
   UINT rightKeySpace = 0;

   BtLeafPage* newLeafPage = nullptr;
   *newPage = nullptr;

   if (mergeOnRight)
   {
	 btr = ExtractLiveRecords(leftSortedArr, leftRecCount, leftKeySpace);
	 leftBase = (char*)(this);
	 btr = otherPage->ExtractLiveRecords(rightSortedArr, rightRecCount, rightKeySpace);
	 rightBase = (char*)(otherPage);
   }
   else
   {
	 btr = otherPage->ExtractLiveRecords(leftSortedArr, leftRecCount, leftKeySpace);
	 leftBase = (char*)(otherPage);
	 btr = ExtractLiveRecords(rightSortedArr, rightRecCount, rightKeySpace);
	 rightBase = (char*)(this);
   }

   UINT keySpace = leftKeySpace + rightKeySpace;
   UINT pageSize = m_Btree->ComputeLeafPageSize(leftRecCount + rightRecCount, keySpace, 0);
   HRESULT hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&newLeafPage), MemObjectType::LeafPage);
   if (hr != S_OK)
   {
	 btr = BT_OUT_OF_MEMORY;
	 goto exit;
   }
   new(newLeafPage)BtLeafPage(pageSize, m_Btree);

   // Insert the records into the new page
   for (UINT i = 0; i < leftRecCount; i++)
   {
	 KeyPtrPair* pre = &leftSortedArr[i];
	 char* key = leftBase + pre->m_KeyOffset;
	 newLeafPage->AppendToSortedSet(key, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
   }
   for (UINT i = 0; i < rightRecCount; i++)
   {
	 KeyPtrPair* pre = &rightSortedArr[i];
	 char* key = rightBase + pre->m_KeyOffset;
	 newLeafPage->AppendToSortedSet(key, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
   }
   *newPage = newLeafPage;

 exit:
   if (leftSortedArr) m_Btree->m_MemoryBroker->DeallocateNow(leftSortedArr, MemObjectType::TmpPointerArray);
   if (rightSortedArr) m_Btree->m_MemoryBroker->DeallocateNow(rightSortedArr, MemObjectType::TmpPointerArray);

   if (btr != BT_SUCCESS)
   {
	 if (newLeafPage) m_Btree->m_MemoryBroker->DeallocateNow(newLeafPage, MemObjectType::LeafPage);
	 newLeafPage = nullptr;
   }

   return btr;

 }

int DefaultCompareKeys(const void* key1, const int keylen1, const void* key2, const int keylen2)
{
  size_t minlen = min(keylen1, keylen2);
  int res = strncmp((char*)(key1), (char*)(key2), minlen);
  if (res == 0)
  {
	// The shorter one is earlier in sort ordere
    if (keylen1 < keylen2) res = -1;
	else if (keylen1 > keylen2) res = 1;
  }
  return res;
}
