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

int BtLeafPage::KeySearchGE(char* searchKey, UINT keyLen)
{
     char* baseAddr = (char*)(this);

     CreatePermutationArray();

     // Do a linear serch - use this later for testing binary search.
    int indx = -1;
    KeyPtrPair* pre = nullptr;
    char* curKey = nullptr;
    int cv = 0;
    for (UINT pos = 0; UINT(pos) < m_PermArr->m_nrEntries; pos++)
    {
        indx = m_PermArr->m_PermArray[pos];
        pre = GetKeyPtrPair(indx);
        curKey = baseAddr + pre->m_KeyOffset;
        cv = m_Btree->m_CompareFn(searchKey, keyLen, curKey, pre->m_KeyLen);
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

BTRESULT BtLeafPage::CreatePermutationArray()
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
    new(permArr) PermutationArray(this, count, initStatus);
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

    if (errorCount > 0)
    {
        PrintPage(stdout, 0);
    }
 
    return errorCount;
}

void BtLeafPage::ShortPrint(FILE* file)
{
    fprintf(file, " LP@0x%llx, %dB, %d+%d", UINT64(this), m_PageSize, m_nSortedSet, m_PageStatus.m_Status.m_nUnsortedReserved);

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
  UINT keySpace = m_PageSize - m_PageStatus.m_Status.m_FirstFreeByte -1;
  UINT32 freeSpace = m_PageSize - PageHeaderSize() - arrSpace - keySpace;
  
  fprintf(file, "Size %d, Usage:  hdr %d array(%d+%d) %d keys %d free %d deleted %d\n", 
	m_PageSize, PageHeaderSize(), m_nSortedSet, nUnsorted, arrSpace, keySpace, freeSpace, delSpace);
  fprintf(file, "State: %d, reserved %d, cleared %d, first free %d\n", 
	m_PageStatus.m_Status.m_PageState, m_PageStatus.m_Status.m_nUnsortedReserved,
	m_PageStatus.m_Status.m_SlotsCleared, m_PageStatus.m_Status.m_FirstFreeByte);

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

void BtIndexPage::ShortPrint(FILE* file)
{
    fprintf(file, " IP@0x%llx, %dB, %d", UINT64(this), m_PageSize, m_nSortedSet);
    KeyPtrPair* pre = GetSortedEntry(0);
    char* sep = (char*)(this) + pre->m_KeyOffset;
    fprintf(file, " \"%1.*s\",", pre->m_KeyLen, sep);

    pre = GetSortedEntry(m_nSortedSet-2);
    sep = (char*)(this) + pre->m_KeyOffset;
    fprintf(file, " \"%1.*s\" ", pre->m_KeyLen, sep);
}

UINT BtIndexPage::CheckPage(FILE* file, KeyType* lowBound, KeyType* hiBound)
{

    KeyPtrPair* pre = nullptr; 
    char* curKey = nullptr;
    UINT  curKeyLen = 0;
    char* prevKey = nullptr;
    UINT  prevKeyLen = 0;
    UINT  errorCount = 0;
 
     for (UINT i = 0; i < m_nSortedSet; i++)
    {
        pre = GetSortedEntry(i);
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


    // Recursively check the subtrees rooted on this page
    KeyType lb;
    KeyType hb;
   
    lb = lowBound;
    for (UINT i = 0; i < m_nSortedSet; i++)
    {
        pre = GetSortedEntry(i);
        curKey = (pre->m_KeyLen > 0) ? (char*)(this) + pre->m_KeyOffset : nullptr;
        curKeyLen = pre->m_KeyLen;
        new(&hb)KeyType(curKey, curKeyLen);

        if (pre->m_RecPtr)
        {
            errorCount += ((BtBasePage*)(pre->m_RecPtr))->CheckPage(file, &lb, &hb);
        }

        lb = hb;
    }
    return errorCount;
}


void BtIndexPage::PrintPage(FILE* file, UINT level)
{
  fprintf(file, "\n------ Index page at level %d -----------------\n", level);

  UINT nUnsorted = m_PageStatus.m_Status.m_nUnsortedReserved;
  UINT delSpace = 0;
  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pre = GetSortedEntry(i);
	if (!pre->m_RecPtr)
	{
	  delSpace += pre->m_KeyLen + sizeof(KeyPtrPair);
	}
  }

  UINT arrSpace = (m_nSortedSet + nUnsorted) * sizeof(KeyPtrPair);
  UINT keySpace = m_PageSize - m_PageStatus.m_Status.m_FirstFreeByte -1;
  UINT32 freeSpace = m_PageSize - PageHeaderSize() - arrSpace - keySpace;

  fprintf(file, "Size %d, Usage:  hdr %d array(%d+%d) %d keys %d free %d deleted %d\n",
	m_PageSize, PageHeaderSize(), m_nSortedSet, nUnsorted, arrSpace, keySpace, freeSpace, delSpace);
  fprintf(file, "State: %d, reserved %d, cleared %d, first free %d\n",
	m_PageStatus.m_Status.m_PageState, m_PageStatus.m_Status.m_nUnsortedReserved,
	m_PageStatus.m_Status.m_SlotsCleared, m_PageStatus.m_Status.m_FirstFreeByte);

  char* baseAddr = (char*)(this);

  fprintf(file, "%d records in sorted area\n", m_nSortedSet);
  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pe = GetSortedEntry(i);
	fprintf(file, "  (%d, %d, 0x%llx):", pe->m_KeyOffset, pe->m_KeyLen, ULONGLONG(pe->m_RecPtr));
   
	char* keyPtr =( pe->m_KeyLen > 0)? (baseAddr + pe->m_KeyOffset): "----------------";
    fprintf(file, " \"%1.*s\",", pe->m_KeyLen, keyPtr);

    BtBasePage* bp = (BtBasePage*)(pe->m_RecPtr);
    if (bp->IsIndexPage()) ((BtIndexPage*)(bp))->ShortPrint(file);
    if (bp->IsLeafPage()) ((BtLeafPage*)(bp))->ShortPrint(file);
    fprintf(file, "\n");
  }

  fprintf(file, "\n----------------------------------------\n");

  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pe = GetSortedEntry(i);
    BtBasePage* pg = (BtBasePage*)(pe->m_RecPtr);
    if (pg->IsIndexPage()) pg->PrintPage(file, level+1);
    //if (pg->IsLeafPage()) ((BtLeafPage*)pg)->ShortPrint(file);
  }
}


void BtreeRoot::Print(FILE* file)
{
  BtreeRootInternal* root = (BtreeRootInternal*)(this);
  root->Print(file);

}

BtBasePage* BtreeRootInternal::CreateEmptyPage()
{
  BtLeafPage* page = nullptr;
  HRESULT hre = m_MemoryBroker->Allocate(m_MinPageSize, (void**)(&page), MemObjectType::LeafPage);
  if (page)
  {
	new(page) BtLeafPage(m_MinPageSize, this);
  }

  return page;
}

BtIndexPage* BtreeRootInternal::CreateIndexPage(BtBasePage* leftPage, BtBasePage* rightPage, char* separator, UINT sepLen)
{

  BtIndexPage* page = nullptr;
  HRESULT hre = m_MemoryBroker->Allocate(m_MinPageSize, (void**)(&page), MemObjectType::IndexPage);
  if (page)
  {
	new(page) BtIndexPage(m_MinPageSize, this);
  }

  // Store separator key first
  UINT offset = page->m_PageStatus.m_Status.m_FirstFreeByte - sepLen + 1;
  page->m_PageStatus.m_Status.m_FirstFreeByte = offset - 1;
  memcpy((char*)(page)+offset, separator, sepLen);

  page->m_nSortedSet = 2;
  KeyPtrPair* prel = page->GetSortedEntry(0);
  prel->Set(offset, sepLen, leftPage);

  KeyType hibound;
  KeyType::GetMaxValue(hibound.m_pKeyValue, hibound.m_KeyLen);
  offset -= hibound.m_KeyLen;
  memcpy((char*)(page)+offset, hibound.m_pKeyValue, hibound.m_KeyLen);
  KeyPtrPair* prer = page->GetSortedEntry(1);
  prer->Set(offset, hibound.m_KeyLen, rightPage);

  return page;
}

// Return index of the entry with the smallest key equal to or greater than the search key
int BtIndexPage::KeySearchGE(KeyType* searchKey )
{
  // On index pages, an entry points to a page containing records with keys that are less than or equal to the separator
  _ASSERTE(m_nSortedSet >= 2);

  char* baseAddr = (char*)(this); 

  // Do a linear serch - use this later for testing binary search.
  int pos = -1;
  KeyPtrPair* pre = nullptr;
  char* curKey = nullptr;
  int cv = 0;
  for (pos = 0; UINT(pos) < UINT(m_nSortedSet-1); pos++)
  {
	pre = GetSortedEntry(pos); 
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

UINT BtIndexPage::AppendToSortedSet(char* separator, UINT sepLen, void* ptr)
{
    m_PageStatus.m_Status.m_FirstFreeByte -= sepLen;
    char* dst = (char*)(this) + m_PageStatus.m_Status.m_FirstFreeByte + 1;
    memcpy_s(dst, sepLen, separator, sepLen);

    m_nSortedSet++;    
    KeyPtrPair* pre = GetSortedEntry(m_nSortedSet-1);
    pre->Set(m_PageStatus.m_Status.m_FirstFreeByte + 1, sepLen, ptr);
 
    return m_nSortedSet;
}


// Create a new index page that includes all the old entries plus the new one.
// The separator equals the highest key value on the left page.
// leftPage and rightPage are the result of splitting the page in position oldPos on this page
BTRESULT BtIndexPage::ExpandIndexPage(char* separator, UINT sepLen, BtBasePage* leftPage, BtBasePage* rightPage, UINT oldPos, BtIndexPage*& newPage)
{
    BTRESULT btr = BT_SUCCESS;
    UINT pageSize = ComputePageSize(m_nSortedSet + 1, KeySpaceSize() + sepLen);
    BtIndexPage* newpage = nullptr;
    HRESULT hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&newpage), MemObjectType::IndexPage);
    if (hr != S_OK) 
    { 
        btr = (hr == E_OUTOFMEMORY) ? BT_OUT_OF_MEMORY : BT_INTERNAL_ERROR;
        goto exit; 
    }

    new(newpage)BtIndexPage(pageSize, m_Btree);
    if (pageSize >= m_Btree->m_MaxPageSize)
    {
       newpage->m_PageStatus.m_Status.m_PageState = SPLIT_PAGE;
    }

    char* curSep = nullptr;
    UINT curSepLen = 0;
    KeyPtrPair* pre = nullptr;

    UINT spos = 0;
    for (UINT tpos = 0; tpos < UINT(m_nSortedSet+1); tpos++)
    {
        if (tpos != oldPos)
        {
            pre = GetSortedEntry(spos);
            curSep = (char*)(this) + pre->m_KeyOffset;
            newpage->AppendToSortedSet(curSep, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
            spos++;
        }
        else
        {
            newpage->AppendToSortedSet(separator, sepLen, leftPage);
        }
    }
    // The entry in OldPos points to the old index page (the one that got split)
    // so we must set it to point to the corresonding new page
    newpage->GetSortedEntry(oldPos + 1)->m_RecPtr = rightPage;

    newPage = newpage;

exit:
    return btr;
}


BTRESULT BtIndexPage::SplitIndexPage(BtIterator* iter)
{
    // The page is assumed to be closed at this point so its state remains unchanged

    BTRESULT btr = BT_SUCCESS;

    UINT lCount = m_nSortedSet / 2;
    UINT rCount = m_nSortedSet - lCount;

    // Copy first lCount records to the left new page (lowever keys)
    UINT keySpace = 0;
    for (UINT i = 0; i < lCount; i++) keySpace += GetSortedEntry(i)->m_KeyLen;
    UINT pageSize = m_Btree->ComputePageSize(lCount, keySpace, 0);

    BtIndexPage* leftPage = nullptr;
    HRESULT hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&leftPage), MemObjectType::LeafPage);
    if (hr != S_OK) 
    { 
        btr = (hr == E_OUTOFMEMORY) ? BT_OUT_OF_MEMORY : BT_INTERNAL_ERROR;
        goto exit; 
    }

    new(leftPage)BtIndexPage(pageSize, m_Btree);
    leftPage->m_nSortedSet = lCount;
    UINT keyOffset = leftPage->PageSize();
    for (UINT i = 0; i < lCount; i++)
    {
        KeyPtrPair* pre = GetSortedEntry(i);
        keyOffset -= pre->m_KeyLen;
        char* dest = (char*)(leftPage)+keyOffset;
        char* src = (char*)(this) + pre->m_KeyOffset;
        memcpy(dest, src, pre->m_KeyLen);
        leftPage->GetSortedEntry(i)->Set(keyOffset, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
    }
    leftPage->m_PageStatus.m_Status.m_FirstFreeByte = keyOffset - 1;

    // Copy the higher rCount records into the right new page (higher keys)
    keySpace = 0;
    for (UINT i = lCount; i < m_nSortedSet; i++) keySpace += GetSortedEntry(i)->m_KeyLen;
    pageSize = m_Btree->ComputePageSize(rCount, keySpace,0);

    BtIndexPage* rightPage = nullptr;
    hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&rightPage), MemObjectType::LeafPage);
    if (hr != S_OK) 
    { 
       btr = (hr == E_OUTOFMEMORY) ? BT_OUT_OF_MEMORY : BT_INTERNAL_ERROR;
       goto exit; 
    }

    new(rightPage)BtIndexPage(pageSize, m_Btree);
    rightPage->m_nSortedSet = rCount;
    keyOffset = rightPage->PageSize();
    for (UINT i = lCount; i < m_nSortedSet; i++)
    {
        KeyPtrPair* pre = GetSortedEntry(i);
        keyOffset -= pre->m_KeyLen;
        char* dest = (char*)(rightPage)+keyOffset;
        char* src = (char*)(this) + pre->m_KeyOffset;
        memcpy(dest, src, pre->m_KeyLen);
        rightPage->GetSortedEntry(i - lCount)->Set(keyOffset, pre->m_KeyLen, const_cast<void*>(pre->m_RecPtr));
    }
    rightPage->m_PageStatus.m_Status.m_FirstFreeByte = keyOffset - 1;

     // Use the last key of the right page as separator for the two pages.
    // A separator thus indicates the highest key value allowed on a page.
    // The separator will be added to the parent index page.
    char* separator = (char*)(leftPage)+leftPage->GetSortedEntry(lCount - 1)->m_KeyOffset;
    UINT seplen = leftPage->GetSortedEntry(lCount - 1)->m_KeyLen;

    
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


    // Now install the two new pages
    if (iter->m_Count == 1)
    {
        // Btree object is the parent so update there
        BtIndexPage* indxPage = m_Btree->CreateIndexPage(leftPage, rightPage, separator, seplen);
        m_Btree->m_RootPage = indxPage;
        m_Btree->m_nIndexPages++;
        fprintf(stdout, "==== New root page ======\n");
        indxPage->PrintPage(stdout, 1);
    }
    else
    {
        // Create a new instance of the parent index page that includes the new separator 
        // and the two new pages. Then update the pointer in the grandparent page.
        BtIndexPage* parentPage = (BtIndexPage*)(iter->m_Path[iter->m_Count - 2].m_Page);
        UINT oldPos = iter->m_Path[iter->m_Count - 2].m_Slot;
        BtIndexPage* newIndxPage = nullptr;
        hr = parentPage->ExpandIndexPage(separator, seplen, leftPage, rightPage, oldPos, newIndxPage);

        BtBasePage** installAddr = nullptr;
        if (iter->m_Count == 2)
        {
            // The b-tree object is the grandparent
            installAddr = const_cast<BtBasePage**>(&m_Btree->m_RootPage);
            fprintf(stdout, "====== New root page ======\n");
            newIndxPage->PrintPage(stdout, 1);
        }
        else
        {
            BtIndexPage* grandParentPage = (BtIndexPage*)(iter->m_Path[iter->m_Count - 3].m_Page);
            KeyPtrPair* recEntry = grandParentPage->GetSortedEntry(iter->m_Path[iter->m_Count - 3].m_Slot);
            installAddr = (BtBasePage**)(&recEntry->m_RecPtr);
        }
        *installAddr = newIndxPage;
        m_Btree->m_EpochMgr->Deallocate(parentPage, MemObjectType::IndexPage); 
        m_Btree->m_nIndexPages++;
    }


exit:
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
        BtIndexPage* indxPage = (BtIndexPage*)(curPage);
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
        }
 
        curPage = (BtBasePage*)(indxPage->GetSortedEntry(slot)->m_RecPtr);
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
        BtBasePage* page = CreateEmptyPage();
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
    }


    // Locate the target leaf page for the insertion
    btr = FindTargetPage(key, &iter);
    BtLeafPage* leafPage = (BtLeafPage*)(iter.m_Path[iter.m_Count-1].m_Page);
    _ASSERTE(leafPage && btr == BT_SUCCESS);

     btr = leafPage->AddRecordToPage(key, recptr);
     if (btr == BT_SUCCESS) 
     {
         m_nRecords++;
         goto exit; 
     }

    // Page is full so either enlarge and consolidate it or split it
    // Try consolidation first 
     BtLeafPage* newPage = nullptr;
     btr = leafPage->ConsolidateLeafPage(newPage, &iter, key->m_KeyLen+sizeof(KeyPtrPair) );
     if (btr == BT_SUCCESS)
     {
        m_EpochMgr->Deallocate(leafPage, MemObjectType::LeafPage);
        m_nConsolidations++;
        CheckTree(stdout); 
        goto tryagain;
     }

     // Consolidation failed so try to split the page instead
    btr = leafPage->SplitLeafPage(&iter);
    if (btr == BT_SUCCESS)
    {
        m_EpochMgr->Deallocate(leafPage, MemObjectType::LeafPage);
        m_nPageSplits++;
        CheckTree(stdout); 
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
        if (leafPage->m_PageSize > m_MinPageSize && leafPage->m_WastedSpace > leafPage->m_PageSize*m_FreeSpaceFraction)
        {
            BtLeafPage* newPage = nullptr;
            btr = leafPage->ConsolidateLeafPage(newPage, &iter, 0);
            if (btr == BT_SUCCESS)
            {
                m_EpochMgr->Deallocate(leafPage, MemObjectType::LeafPage);
                m_nConsolidations++;
                CheckTree(stdout);
            }
        } else
        if (leafPage->m_PageSize <= m_MinPageSize)
        {
            //Try to merge page with left or right neighbor
        }
        if (leafPage->LiveRecordCount() == 0)
        {
            //DeleteLeafPage(leafPage, &iter);
        }
        m_nRecords--;
        goto exit;
    }

exit:
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
    BtLeafPage* leafPage = (BtLeafPage*)(iter.m_Path[iter.m_Count - 1].m_Page);
    _ASSERTE(leafPage);
    if (!leafPage)
    {
        btr = BT_INTERNAL_ERROR;
        goto exit;
    }

    // Found the target leaf page, now look for the record
    int pos = leafPage->KeySearchGE((char*)(key->m_pKeyValue), key->m_KeyLen);
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
        errorCount = rootPage->CheckPage(file, &lowbound, &hibound);
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
    newState.m_Status.m_FirstFreeByte -= key->m_KeyLen;
    LONG64 oldval = InterlockedCompareExchange64((LONG64*)(&m_PageStatus.m_Status64), newState.m_Status64, pst.m_Status64);
    if (oldval != pst.m_Status64)
    {
	    goto tryagain;
    }
 
 
  // First copy the key into its reserved space
  char* keyBuffer = (char*)(this) + newState.m_Status.m_FirstFreeByte + 1;
  memcpy(keyBuffer, key->m_pKeyValue, key->m_KeyLen);

  // Then fill in the record slot
  UINT32 slotIndx = newState.m_Status.m_nUnsortedReserved - 1;
  KeyPtrPair* pentry = GetUnsortedEntry(slotIndx); 
  pentry->m_KeyOffset = newState.m_Status.m_FirstFreeByte + 1;
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
    int pos = KeySearchGE((char*)(key->m_pKeyValue), key->m_KeyLen);
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

  UINT reqSpace = sizeof(KeyPtrPair) + keylen;
  UINT arrSpace = (m_nSortedSet + pst.m_Status.m_nUnsortedReserved) * sizeof(KeyPtrPair);
  UINT keySpace = m_PageSize - pst.m_Status.m_FirstFreeByte;
  UINT freeSpace = m_PageSize - PageHeaderSize() - arrSpace - keySpace;  

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

BTRESULT BtLeafPage::ConsolidateLeafPage(BtLeafPage*& newpage, BtIterator* iter, UINT minFree)
{
tryagain:

    // Get a stable copy of the page status
    PageStatusUnion pst;
    pst.m_Status64 = m_PageStatus.m_Status64;

    newpage = nullptr;
    KeyPtrPair* sortArr = nullptr;

    BTRESULT btr = BT_SUCCESS;
    BtLeafPage* newPage = nullptr;


    // Count the number of records remaining on the page and the amount of space needed for the new page
    UINT recCount = 0;
    UINT keySpace = 0;
    KeyPtrPair* pre = nullptr;
    for (UINT i = 0; i < m_nSortedSet; i++)
    {
        pre = GetKeyPtrPair(i);
        if (pre->m_RecPtr)
        {
            recCount++;
            keySpace += pre->m_KeyLen;
        }
    }

    UINT nrUnsorted = 0;
    for (UINT i = 0; i < pst.m_Status.m_nUnsortedReserved; i++)
    {
        pre = GetUnsortedEntry(i);
        if (pre->m_RecPtr)
        {
            recCount++;
            keySpace += pre->m_KeyLen;
            nrUnsorted++;
        }
    }

    UINT newSize = m_Btree->ComputePageSize(recCount, keySpace, minFree);

    // Don't expand beyond maximum page size
    if (newSize > m_Btree->m_MaxPageSize)
    {
        btr = BT_MAX_SIZE;
        goto exit;
    }

    if (pst.m_Status64 != m_PageStatus.m_Status64)
    {
        goto tryagain;
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
    newPage->m_nSortedSet = recCount;

    // Copy the unsorted record entries into an array and sort them
    hr = m_Btree->m_MemoryBroker->Allocate(nrUnsorted*sizeof(KeyPtrPair), (void**)(&sortArr), MemObjectType::TmpPointerArray);
    if (hr != S_OK)
    {
        btr = BT_OUT_OF_MEMORY;
        goto exit;
    }

    for (UINT i = 0; i < nrUnsorted; i++)
    {
        pre = GetUnsortedEntry(i);
        sortArr[i] = *pre;
    }
    btr = SortUnsortedSet(sortArr, nrUnsorted);


    UINT32 keyOffset = newSize - 1;

    // Merge the record entries from the sorted set on the input page
    // and the record entries from the sort array to create the sorted set of the new page
    KeyPtrPair* pLeft = GetKeyPtrPair(0);
    KeyPtrPair* pRight = &sortArr[0];
    KeyPtrPair* pTarget = newPage->GetKeyPtrPair(0);
    INT lCount = m_nSortedSet;
    INT rCount = nrUnsorted;

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
            keyOffset -= pLeft->m_KeyLen;
            char* keyAddr = (char*)(newPage)+keyOffset;
            memcpy(keyAddr, lKey, pLeft->m_KeyLen);

            // Update target entry
            pTarget->Set(keyOffset, pLeft->m_KeyLen, const_cast<void*>(pLeft->m_RecPtr));

            // Advance left inputf and target
            pTarget++;
            pLeft++;
            lCount--;
        }
        else
        {
            // Copy record entry and key value from left input
            keyOffset -= pRight->m_KeyLen;
            char* keyAddr = (char*)(newPage)+keyOffset;
            memcpy(keyAddr, rKey, pRight->m_KeyLen);

            // Update target entry
            pTarget->Set(keyOffset, pRight->m_KeyLen, const_cast<void*>(pRight->m_RecPtr));

            // Advance left inputf and target
            pTarget++;
            pRight++;
            rCount--;
        }
    }

    // Copy remaining, if any, from left or right
    while (lCount > 0)
    {
        // Copy record entry and key value from left input
        char* lKey = (char*)(this) + pLeft->m_KeyOffset;
        keyOffset -= pLeft->m_KeyLen;
        char* keyAddr = (char*)(newPage)+keyOffset;
        memcpy(keyAddr, lKey, pLeft->m_KeyLen);

        // Update target entry
        pTarget->Set(keyOffset, pLeft->m_KeyLen, const_cast<void*>(pLeft->m_RecPtr));

        // Advance left inputf and target
        pTarget++;
        pLeft++;
        lCount--;
    }

    while (rCount > 0)
    {
        // Copy record entry and key value from right input
        char* rKey = (char*)(this) + pRight->m_KeyOffset;
        keyOffset -= pRight->m_KeyLen;
        char* keyAddr = (char*)(newPage)+keyOffset;
        memcpy(keyAddr, rKey, pRight->m_KeyLen);

        // Update target entry
        pTarget->Set(keyOffset, pRight->m_KeyLen, const_cast<void*>(pRight->m_RecPtr));

        // Advance left inputf and target
        pTarget++;
        pRight++;
        rCount--;
    }
    newPage->m_PageStatus.m_Status.m_FirstFreeByte = keyOffset - 1;

    for (UINT i = 0; i < newPage->m_nSortedSet; i++)
    {
        newPage->m_RecordArr[i].CleanEntry();
    }

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
            BtIndexPage* parentPage = (BtIndexPage*)(iter->m_Path[iter->m_Count - 2].m_Page);
            KeyPtrPair* recEntry = parentPage->GetSortedEntry(iter->m_Path[iter->m_Count - 2].m_Slot);
            installAddr = (BtBasePage**)(&recEntry->m_RecPtr);
        }

        LONG64 oldval = InterlockedCompareExchange64((LONG64*)(installAddr), LONG64(newPage), LONG64(this));
        if (oldval != LONG64(this))
        {
            btr = BT_INSTALL_FAILED;
        }
    }

exit:
    if (sortArr) m_Btree->m_EpochMgr->DeallocateNow(sortArr, MemObjectType::TmpPointerArray);
    sortArr = nullptr;

    if (btr != BT_SUCCESS)
    {
        if (newPage) m_Btree->m_EpochMgr->Deallocate(newPage, MemObjectType::LeafPage);
        newPage = nullptr;
    }
    newpage = newPage;
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

  // Copy first lCount records to the left new page (lowever keys)
  UINT keySpace = 0;
  for (UINT i = 0; i < lCount; i++) keySpace += GetKeyPtrPair(i)->m_KeyLen;
  UINT pageSize = m_Btree->ComputePageSize(lCount, keySpace, 0);

  BtLeafPage* leftPage = nullptr;
  hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&leftPage), MemObjectType::LeafPage);
  if (hr != S_OK) 
  { 
      btr = BT_OUT_OF_MEMORY;
      goto exit; 
  }

  new(leftPage)BtLeafPage(pageSize, m_Btree);
  leftPage->m_nSortedSet = lCount;
  UINT keyOffset = leftPage->PageSize();
  for (UINT i = 0; i < lCount; i++)
  {
	keyOffset -= sortArr[i].m_KeyLen;
	char* dest = (char*)(leftPage)+ keyOffset;
	char* src = (char*)(this) + sortArr[i].m_KeyOffset;
	memcpy(dest, src, sortArr[i].m_KeyLen);
	leftPage->GetKeyPtrPair(i)->Set(keyOffset, sortArr[i].m_KeyLen, const_cast<void*>(sortArr[i].m_RecPtr));
  }
  leftPage->m_PageStatus.m_Status.m_FirstFreeByte = keyOffset - 1;

  // Copy the higher rCount records into the right new page (higher keys)
  keySpace = 0;
  for (UINT i = lCount; i < nrRecords; i++) keySpace += GetKeyPtrPair(i)->m_KeyLen;
  pageSize = m_Btree->ComputePageSize(rCount, keySpace, 0);

  BtLeafPage* rightPage = nullptr;
  hr = m_Btree->m_MemoryBroker->Allocate(pageSize, (void**)(&rightPage), MemObjectType::LeafPage);
  if (hr != S_OK) 
  { 
      btr = BT_OUT_OF_MEMORY;
      goto exit; 
  }

  new(rightPage)BtLeafPage(pageSize, m_Btree);
  rightPage->m_nSortedSet = rCount;
  keyOffset = rightPage->PageSize();
  for (UINT i = lCount; i < nrRecords; i++)
  {
	keyOffset -= sortArr[i].m_KeyLen;
	char* dest = (char*)(rightPage)+ keyOffset;
	char* src = (char*)(this) + sortArr[i].m_KeyOffset;
	memcpy(dest, src, sortArr[i].m_KeyLen);
	rightPage->GetKeyPtrPair(i - lCount)->Set(keyOffset, sortArr[i].m_KeyLen, const_cast<void*>(sortArr[i].m_RecPtr));
  }
  rightPage->m_PageStatus.m_Status.m_FirstFreeByte = keyOffset - 1;
 
  // Use the last key of the left page as separator for the two pages.
  // A separator thus indicates the highest key value allowed on a page.
  // The separator will be added to the parent index page.
  char* separator = (char*)(leftPage)+leftPage->GetKeyPtrPair(lCount-1)->m_KeyOffset;
  UINT seplen = leftPage->GetKeyPtrPair(lCount-1)->m_KeyLen;

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

  // Now install the two new pages
  if (iter->m_Count == 1)
  {
      // Btree object is the parent so update there
      BtIndexPage* indxPage = m_Btree->CreateIndexPage(leftPage, rightPage, separator, seplen);
      m_Btree->m_RootPage = indxPage;
      m_Btree->m_nIndexPages++;
  }
  else
  {
      // Create a new instance of the parent index page that includes the new separator 
      // and the two new pages. Then update the pointer in the grandparent page.
      BtIndexPage* parentPage = (BtIndexPage*)(iter->m_Path[iter->m_Count - 2].m_Page);
      UINT oldPos = iter->m_Path[iter->m_Count - 2].m_Slot;
      BtIndexPage* newIndxPage = nullptr;
      hr = parentPage->ExpandIndexPage(separator, seplen, leftPage, rightPage, oldPos, newIndxPage);

      BtBasePage** installAddr = nullptr;
      if (iter->m_Count == 2)
      {
          // The b-tree object is the grandparent
          installAddr = const_cast<BtBasePage**>(&m_Btree->m_RootPage);
      }
      else
      {
          BtIndexPage* grandParentPage = (BtIndexPage*)(iter->m_Path[iter->m_Count - 3].m_Page);
          KeyPtrPair* recEntry = grandParentPage->GetSortedEntry(iter->m_Path[iter->m_Count - 3].m_Slot);
          installAddr = (BtBasePage**)(&recEntry->m_RecPtr);
      }
      // TODO - make this atomic
      *installAddr = newIndxPage;
       m_Btree->m_EpochMgr->Deallocate(parentPage, MemObjectType::IndexPage);
  }
  m_Btree->m_nLeafPages++;

 exit:
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
