#include <Windows.h>
#include "MemoryAllocator.h"
#include "MemoryBroker.h"
#include "mwCAS.h"
#include "BtreeInternal.h"



void TraceInfo::Print(FILE* file)
{
  fprintf(file, "****** Trace info *******\n");

  fprintf(file, "------ Home page ----\n");
  if (m_HomePage) m_HomePage->PrintLeafPage(file, 0);

  char * ac = nullptr;
  for (int i = 0; i < m_ActionCount; i++)
  {
	Action* pa = &m_ActionArr[i];
	switch (pa->m_ActionType)
	{
	case NO_ACTION: ac = "NO_ACTON"; break;
	case CONS_PAGE: ac = "CONS_PAGE"; break;
	case SPLI_PAGE: ac = "SPLIT_PAGE"; break;
	case MERGE_PAGE: ac = "MERGE_PAGE"; break;
	}
	fprintf(file, "Src page, %s \n", ac);
	if (pa->m_TrgtPage)
	{
	  pa->m_TrgtPage->PrintLeafPage(file, 0);
	}
	if (pa->m_ResPage1)
	{
	  fprintf(file, "\nResult page 1\n");
	  pa->m_ResPage1->PrintLeafPage(file, 0);
	}
	if (pa->m_ResPage2)
	{
	  fprintf(file, "\nResult page 2\n");
	  pa->m_ResPage2->PrintLeafPage(file, 0);
	}
  }


  fprintf(file, "*************************\n");
}



int BtreePage::KeySearch(KeyType* searchKey, BtreePage::CompType ctype, bool forwardScan)
{
 
  
tryagain:

  PermutationArray* permArr = nullptr;
  LONGLONG bpsw = m_PageStatus.ReadLL();
  PageStatus* bpst = (PageStatus*)(&bpsw); 
  //_ASSERTE(bpst->m_PageState == PAGE_NORMAL);
  
  BTRESULT bt = CreatePermutationArray(permArr);
  _ASSERTE(bt == BT_SUCCESS);

  // Forward or backward scan?
  int first = 0;
  int last = (m_PageType == LEAF_PAGE)? permArr->m_nrEntries: m_nSortedSet;
  int incr = 1;
  if (ctype == LTE || ctype == LT)
  {
	// Do a backwards scan
	first = last-1;
	last = 0;
	incr = -1;
  }

     // Do a linear serch - use this later for testing binary search.
    char* baseAddr = (char*)(this);
 
    int indx = -1;
    KeyPtrPair* pre = nullptr;
    char* curKey = nullptr;
    int cv = 0;
	int cv2 = 0;
    for (int pos = first; pos != last; pos += incr)
    {
        indx = permArr->m_PermArray[pos];
        pre = GetKeyPtrPair(indx);
        curKey = baseAddr + pre->m_KeyOffset;
        cv = m_Btree->m_CompareFn(searchKey->m_pKeyValue, searchKey->m_KeyLen, curKey, pre->m_KeyLen);
		// Adjust for direction
		cv = incr*cv;

        if (IsIndexPage())
        {
            if (cv <= 0 )
            {
			  // Special case when scanning backwards in an index page
			  if (cv == 0 && incr == -1)
			  {
				indx++;
			  }
			  break;
            }
        }
        else
        {
			// Scanning a leaf page
		  switch (ctype)
		  {
		  case LT: case GT:
			if (cv <= 0)
			{
			  indx -= incr;
			  goto loopend;
			}
			break;
		  case LTE: case GTE:
           if (cv < 0)
            {
                indx -= incr;
                goto loopend;
            }
		   break;
		  case EQ:
			if (cv < 0)
			{
			  indx = -1;
			  goto loopend;
			}
           if (cv == 0)
            {
               goto loopend;
            }
		   break;
		  }
 
 
        }
    }
  loopend:

    if (permArr->m_CreateStatus != 0 && permArr->m_CreateStatus != m_PageStatus)
    {
        goto tryagain;
    }

	LONGLONG epsw = m_PageStatus.ReadLL();
	if (bpsw != epsw)
	{
	  goto tryagain;
	}

	if (m_PageType == INDEX_PAGE && indx >= 0 && bpst->m_PageState == PAGE_NORMAL)
	{
	  int errcnt = 0;
	  // Make sure key separator in slot indx-1 is less than the search key and the one on slot indx is greater than or qual
	  if (indx > 0)
	  {
		pre = GetKeyPtrPair(indx - 1);
		curKey = baseAddr + pre->m_KeyOffset;
		cv = m_Btree->m_CompareFn(searchKey->m_pKeyValue, searchKey->m_KeyLen, curKey, pre->m_KeyLen);
		
		if (cv <= 0)
		{
		  printf("Thread %d: Separator %*s in indx-1 is GTE than search key %*s\n", GetCurrentThreadId(),
			int(pre->m_KeyLen), curKey, int(searchKey->m_KeyLen), searchKey->m_pKeyValue);
		  PrintPage(stdout, 0, false);
		  errcnt++;
		}
	  }
	  pre = GetKeyPtrPair(indx);
	  curKey = baseAddr + pre->m_KeyOffset;
	  int keylen = int(pre->m_KeyLen);
	  cv = m_Btree->m_CompareFn(searchKey->m_pKeyValue, searchKey->m_KeyLen, curKey, pre->m_KeyLen);
	  if (cv > 0)
	  {
		printf("Thread %d: Separator %.*s in slot %d is less than search key %.*s\n",GetCurrentThreadId(),
		  keylen, curKey, indx, int(searchKey->m_KeyLen), searchKey->m_pKeyValue);
		PrintPage(stdout, 0, false);
		errcnt++;
	  }
	  if (errcnt > 0)
	  {
		printf("Stop here\n");
	  }
	}

    if (indx >= 0)
    {
        pre = GetKeyPtrPair(indx);
        if(!pre->m_Pointer.ReadPP())
        {
            indx = -1;
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

static PermutationArray* defaultPermArray = nullptr;


BTRESULT BtreePage::CreatePermutationArray( PermutationArray*& permArr)
{
  if (m_PageType == INDEX_PAGE)
  {
	if (!defaultPermArray)
	{
	  UINT size = sizeof(PermutationArray) + (200 - 1) * sizeof(UINT16);
	  HRESULT hre = m_Btree->m_MemoryBroker->Allocate(size, (void**)&defaultPermArray, MemObjectType::TmpPointerArray);
	  new(defaultPermArray) PermutationArray(nullptr, 200, 0);
	}
	permArr = defaultPermArray;
	return BT_SUCCESS;
  }
    LONGLONG initsw;
	PageStatus* initpst = (PageStatus*)(&initsw);
    BTRESULT btr = BT_SUCCESS;

 
tryagain:
    initsw = m_PageStatus.ReadLL();
    btr = BT_SUCCESS;

    PermutationArray* oldArray = const_cast<PermutationArray*>(m_PermArr);

    // Do we already have a permutation array?
    if (oldArray)
    {
        // We do. Is it still valid?
        if (oldArray->m_CreateStatus == initsw)
        {
		  permArr = oldArray;
          goto exit;
        }
        // Existing array is no longer valid so delete it
        InterlockedCompareExchange64((LONG64*)&m_PermArr, LONG64(0), LONG64(oldArray));
        m_Btree->m_EpochMgr->Deallocate(oldArray, MemObjectType::TmpPointerArray);
    }

    // Need to create a new permutation array
    PermutationArray* newArray = nullptr;
    UINT count = m_nSortedSet + initpst->m_nUnsortedReserved;
    UINT size = sizeof(PermutationArray) + (count - 1)*sizeof(UINT16);
    HRESULT hre =  m_Btree->m_MemoryBroker->Allocate(size, (void**)&newArray, MemObjectType::TmpPointerArray);
    new(newArray) PermutationArray(this, count, initsw);
    if (initpst->m_nUnsortedReserved > 0)
    {
        btr = newArray->SortPermArray();
    }

    // Has new records been added since we filled the permutation array? If so, try again.
	LONGLONG cursw = m_PageStatus.ReadLL();
	PageStatus* curpst = (PageStatus*)(&cursw);
    if (initpst->m_nUnsortedReserved < curpst->m_nUnsortedReserved)
    {
        m_Btree->m_EpochMgr->DeallocateNow(newArray, MemObjectType::TmpPointerArray);
        goto tryagain;
    }

    LONG64 rval = InterlockedCompareExchange64((LONG64*)(&m_PermArr),LONG64(newArray), LONG64(oldArray));
    if (rval != LONG64(oldArray))
    {
        goto tryagain;
    }

	permArr = newArray;
	if (initsw != m_PageStatus.ReadLL())
	{
      m_Btree->m_EpochMgr->DeallocateNow(newArray, MemObjectType::TmpPointerArray);
	  goto tryagain;
	}

  exit:
    return btr;
}

void BtreePage::ComputeLeafStats(BtreeStatistics* statsp)
{
  _ASSERTE(IsLeafPage());

  PageStatus* pst = (PageStatus*)(&m_PageStatus);

  statsp->m_LeafPages++;
  statsp->m_Records += m_nSortedSet + pst->m_nUnsortedReserved - pst->m_SlotsCleared;
  statsp->m_SpaceLP += m_PageSize;
  statsp->m_AllocedSpaceLP += UINT(_msize(this));
  statsp->m_HeaderSpaceLP += PageHeaderSize();
  statsp->m_KeySpaceLP += KeySpaceSize();
  statsp->m_RecArrSpaceLP += (m_nSortedSet+pst->m_nUnsortedReserved) * sizeof(KeyPtrPair);
  statsp->m_FreeSpaceLP += FreeSpace();
  statsp->m_DeletedSpaceLP += m_WastedSpace;
  UINT hsize = UINT((char*)(&m_RecordArr[0]) - (char*)(this));
  _ASSERTE(hsize == PageHeaderSize());
}

UINT BtreePage::CheckLeafPage(FILE* file, KeyType* lowBound, KeyType* hiBound)
{
    KeyPtrPair* pre = nullptr;
    char* curKey = nullptr;
    UINT  curKeyLen = 0;
    char* prevKey = nullptr;
    UINT  prevKeyLen = 0;
    UINT  errorCount = 0;
	LONGLONG psw = m_PageStatus.ReadLL();
	PageStatus* pst = (PageStatus*)(&psw);

    for (UINT i = 0; i < UINT(m_nSortedSet + pst->m_nUnsortedReserved); i++)
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
	  if (psw == m_PermArr->m_CreateStatus)
	  {
		_ASSERTE(m_PermArr->m_TargetPage == this);
		_ASSERTE(m_PermArr->m_nrEntries == m_nSortedSet + pst->m_nUnsortedReserved);
	  }
	}

    if (errorCount > 0)
    {
        PrintPage(stdout, 0);
    }
 
    return errorCount;
}

void BtreePage::ShortPrintLeaf(FILE* file)
{
	LONGLONG psw = m_PageStatus.ReadLL();
	PageStatus* pst = (PageStatus*)(&psw);

    fprintf(file, " LP@0x%llx, %dB, %d+%d", UINT64(this), m_PageSize, m_nSortedSet, pst->m_nUnsortedReserved);

	if (m_nSortedSet + pst->m_nUnsortedReserved > 0)
	{
    KeyPtrPair* pre = GetKeyPtrPair(0);    
    UINT lowKeyLen = pre->m_KeyLen; 
    UINT hiKeyLen  = pre->m_KeyLen;
    char* lowKey   = (char*)(this) + pre->m_KeyOffset;
    char* hiKey    = (char*)(this) + pre->m_KeyOffset;

	  for (UINT i = 1; i < UINT(m_nSortedSet + pst->m_nUnsortedReserved); i++)
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

void BtreePage::PrintLeafPage(FILE* file, UINT level)
{
  LONGLONG psw = m_PageStatus.ReadLL();
  PageStatus* pst = (PageStatus*)(&psw);

  _ASSERTE(IsLeafPage());

  fprintf(file, "\n------ Leaf page 0X%I64X -----------------------\n", ULONGLONG(this));

  UINT nUnsorted = pst->m_nUnsortedReserved;
  UINT delSpace = 0;
  for (UINT i = 0; i < m_nSortedSet ; i++)
  {
	KeyPtrPair* pre = GetKeyPtrPair(i);
	if (!pre->m_Pointer.ReadPP())
	{
	  delSpace += pre->m_KeyLen + sizeof(KeyPtrPair);
	}
  }

  UINT arrSpace = (m_nSortedSet + nUnsorted) * sizeof(KeyPtrPair);
  UINT keySpace = m_PageSize - pst->m_LastFreeByte -1;
  UINT32 freeSpace = m_PageSize - PageHeaderSize() - arrSpace - keySpace;
  
  fprintf(file, "Size %d, Usage:  hdr %d array(%d+%d) %d keys %d free %d deleted %d\n", 
	m_PageSize, PageHeaderSize(), m_nSortedSet, nUnsorted, arrSpace, keySpace, freeSpace, delSpace);
  PageStatus::PrintPageStatus(file, psw);
  fprintf(file, "\n");

  char* baseAddr = (char*)(this);
  KeyPtrPair* pe = nullptr;
  char* keyPtr = nullptr;
  char* recPtr = nullptr;
  
  fprintf(file, "%d records in sorted area\n", m_nSortedSet);
  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	pe = GetKeyPtrPair(i);
	fprintf(file, "  (%d, %d, 0x%llx):", pe->m_KeyOffset, pe->m_KeyLen, pe->m_Pointer.ReadLL());
	keyPtr = (baseAddr + pe->m_KeyOffset);
	pe->CleanEntry();
	recPtr = pe->m_Pointer.Read();
	if (recPtr == nullptr) recPtr =  ".....................";
	fprintf(file, "  \"%1.*s\", \"%s\"\n", pe->m_KeyLen, keyPtr, recPtr);
  }

  fprintf(file, "%d records in unsorted area\n", nUnsorted);
  for (UINT i = 0; i < nUnsorted; i++)
  {
	pe = GetUnsortedEntry(i);
	fprintf(file, "  (%d, %d, 0x%llx):", pe->m_KeyOffset, pe->m_KeyLen, pe->m_Pointer.ReadLL());
	keyPtr = (baseAddr + pe->m_KeyOffset);
	pe->CleanEntry();
	recPtr = pe->m_Pointer.Read();
	if( recPtr == nullptr) recPtr =  ".....................";
	fprintf(file, "  \"%1.*s\", \"%s\"\n", pe->m_KeyLen, keyPtr, recPtr);
  }

  fprintf(file, "\n----------------------------------------\n");
}

void BtreePage::ShortPrint(FILE* file)
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

void BtreePage::ComputeTreeStats(BtreeStatistics* statsp)
{
  _ASSERTE(IsIndexPage());

  statsp->m_IndexPages++;
  statsp->m_SpaceIP += m_PageSize;
  statsp->m_AllocedSpaceIP += UINT(_msize(this));
  statsp->m_HeaderSpaceIP += PageHeaderSize();
  statsp->m_KeySpaceIP += KeySpaceSize();
  statsp->m_RecArrSpaceIP += m_nSortedSet * sizeof(KeyPtrPair);

  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pre = GetKeyPtrPair(i);
	BtreePage* page = pre->m_Pointer.ReadPP();
	if (page->IsIndexPage())
	{
	  page->ComputeTreeStats(statsp);
	}
	else
	{
	  page->ComputeLeafStats(statsp);
	}
  }

}

UINT BtreePage::CheckPage(FILE* file, KeyType* lowBound, KeyType* hiBound)
{
    if (IsLeafPage())
    {
	  CheckLeafPage(file, lowBound, hiBound);
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

            if (pre->m_Pointer.ReadPP())
            {
                BtreePage* pg = pre->m_Pointer.ReadPP();
                errorCount += pg->CheckPage(file, &lb, &hb);
            }

            lb = hb;
        }
    }
    return errorCount;
}


void BtreePage::PrintPage(FILE* file, UINT level, bool recursive)
{
    if (IsLeafPage())
    {
        PrintLeafPage(file, level);
        return;
    }
	LONGLONG psw = m_PageStatus.ReadLL();
	PageStatus* pst = (PageStatus*)(&psw);

  fprintf(file, "\n------ Index page 0X%I64X at level %d -----------------\n", ULONGLONG(this), level);

  UINT nUnsorted = pst->m_nUnsortedReserved;
  UINT delSpace = 0;
  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pre = GetKeyPtrPair(i);
	if (!pre->m_Pointer.Read())
	{
	  delSpace += pre->m_KeyLen + sizeof(KeyPtrPair);
	}
  }

  UINT arrSpace = (m_nSortedSet + nUnsorted) * sizeof(KeyPtrPair);
  UINT keySpace = m_PageSize - pst->m_LastFreeByte -1;
  UINT32 freeSpace = m_PageSize - PageHeaderSize() - arrSpace - keySpace;

  fprintf(file, "Size %d, Usage:  hdr %d array(%d+%d) %d keys %d free %d deleted %d\n",
	m_PageSize, PageHeaderSize(), m_nSortedSet, nUnsorted, arrSpace, keySpace, freeSpace, delSpace);
  PageStatus::PrintPageStatus(file, psw);
  fprintf(file, "\n");

  char* baseAddr = (char*)(this);

  fprintf(file, "%d records in sorted area\n", m_nSortedSet);
  for (UINT i = 0; i < m_nSortedSet; i++)
  {
	KeyPtrPair* pe = GetKeyPtrPair(i);
	fprintf(file, "  (%d, %d, 0x%llx):", pe->m_KeyOffset, pe->m_KeyLen, pe->m_Pointer.ReadLL());
   
	char* keyPtr =( pe->m_KeyLen > 0)? (baseAddr + pe->m_KeyOffset): "----------------";
    fprintf(file, " \"%1.*s\",", pe->m_KeyLen, keyPtr);

    BtreePage* bp = pe->m_Pointer.ReadPP();
    if (bp->IsIndexPage()) bp->ShortPrint(file);
    if (bp->IsLeafPage()) bp->ShortPrintLeaf(file);
    fprintf(file, "\n");
  }

  fprintf(file, "\n----------------------------------------\n");

  if (recursive)
  {
	for (UINT i = 0; i < m_nSortedSet; i++)
	{
	  KeyPtrPair* pe = GetKeyPtrPair(i);
	  BtreePage* pg = pe->m_Pointer.ReadPP();
	  if (true || pg->IsIndexPage()) ((BtreePage*)(pg))->PrintPage(file, level + 1);
	}
  }
}


void BtreeRoot::Print(FILE* file)
{
  BtreeRootInternal* root = (BtreeRootInternal*)(this);
  root->Print(file);

}

BTRESULT BtreeRootInternal::AllocateLeafPage(UINT recCount, UINT keySpace, BtreePage*& newPage)
{
  UINT pageSize = ComputeLeafPageSize(recCount, keySpace, 0);
  pageSize = max(pageSize, m_MinPageSize);

  BtreePage* page = nullptr;
  HRESULT hre = m_MemoryBroker->Allocate(pageSize, (void**)(&page), MemObjectType::LeafPage);
  if (page)
  {
	new(page) BtreePage(BtreePage::LEAF_PAGE, pageSize, this);
	newPage = page;
	return BT_SUCCESS;
  }
  return BT_OUT_OF_MEMORY;
}

BTRESULT BtreeRootInternal::AllocateIndexPage(UINT recCount, UINT keySpace, BtreePage*& newPage)
{
  UINT pageSize = ComputeIndexPageSize(recCount, keySpace);

  BtreePage* page = nullptr;
  HRESULT hre = m_MemoryBroker->Allocate(pageSize, (void**)(&page), MemObjectType::IndexPage);
  if (page)
  {
	new(page) BtreePage(BtreePage::INDEX_PAGE, pageSize, this);
	newPage = page;
	return BT_SUCCESS;
  }
  return BT_OUT_OF_MEMORY;
}

BtreePage* BtreeRootInternal::CreateIndexPage(BtreePage* leftPage, BtreePage* rightPage, char* separator, UINT sepLen)
{
    KeyType hibound;
    KeyType::GetMaxValue(hibound.m_pKeyValue, hibound.m_KeyLen);  

    UINT keySpace = sepLen + hibound.m_KeyLen;

	BtreePage* page = nullptr;
	BTRESULT btr = AllocateIndexPage(2, keySpace, page);
    if (btr != BT_SUCCESS)
    {
         goto exit;
    }

    // Store separator key first and then the high bound
    page->AppendToSortedSet(separator, sepLen, leftPage);
    page->AppendToSortedSet(hibound.m_pKeyValue, hibound.m_KeyLen, rightPage);
    //_ASSERTE(page->FreeSpace() == 0);

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

BtreePage::BtreePage(PageType type, UINT size, BtreeRootInternal* root)
{
    _ASSERTE(size <= 64 * 1024);
	PageStatus* pst = (PageStatus*)(&m_PageStatus);

    memset(this, 0, size);
    m_Btree = root;
    m_PageSize = size;
    m_PageType = type;
    m_nSortedSet = 0;
    m_WastedSpace = 0;
    pst->m_LastFreeByte = size - 1;
    pst->m_nUnsortedReserved = 0;
    pst->m_PageState = PAGE_NORMAL;
    pst->m_SlotsCleared = 0;
	pst->m_PendAction = PA_NONE;
	m_PermArr = nullptr;
#ifdef _DEBUG
	m_SrcPage1 = m_SrcPage2 = nullptr;
	m_TrgtPage1 = m_TrgtPage2 = nullptr;
#endif
}

UINT BtreePage::PageHeaderSize()
{
  BtreePage dummy(LEAF_PAGE, 1, nullptr);
  UINT headerSize = UINT((char*)(&dummy.m_RecordArr[0]) - (char*)(&dummy));
  return headerSize;
}

UINT BtreePage::AppendToSortedSet(char* key, UINT keyLen, void* ptr)
{
	// This function will only be called on a new page that the
	// calling thread has exclusive access to so there is no need
	// to use interlocked instructions to modify page status.
	PageStatus* pst = (PageStatus*)(&m_PageStatus);

    _ASSERTE(FreeSpace() >= keyLen + sizeof(KeyPtrPair));
    pst->m_LastFreeByte -= keyLen;
    char* dst = (char*)(this) + pst->m_LastFreeByte + 1;
    memcpy_s(dst, keyLen, key, keyLen);

    m_nSortedSet++;
    KeyPtrPair* pre = GetKeyPtrPair(m_nSortedSet - 1);
    pre->Set(pst->m_LastFreeByte + 1, keyLen, ptr);
    pre->CleanEntry();

    return m_nSortedSet;
}



// When a leaf page or index page is split, we create a new parent page with room for one new separator-pointer pair.
// The separator equals the highest key value on the left page.
// leftPage and rightPage are the new pages that were created by the split.
// In the old parent page, the split page was reference in position oldPos.
BTRESULT BtreePage::ExpandIndexPage(char* separator, UINT sepLen, BtreePage* leftPage, BtreePage* rightPage, UINT oldPos, BtreePage*& newPage)
{
    _ASSERTE(IsIndexPage());

	BTRESULT btr = BT_SUCCESS;
	LONGLONG psw = m_PageStatus.ReadLL();
	PageStatus* pst = (PageStatus*)(&psw);

    BtreePage* newpage = nullptr;
	btr = m_Btree->AllocateIndexPage(m_nSortedSet + 1, KeySpaceSize() + sepLen, newpage);
     if (btr != BT_SUCCESS) 
    { 
       goto exit; 
    }
 
    // Allow the parent page to be larger than the max page size temporararily
    // but mark it to be split
    if (newpage->PageSize() >= m_Btree->m_MaxPageSize)
    {
	  PageStatus* newpst = (PageStatus*)(&newpage->m_PageStatus);
      newpst->m_PendAction = PA_SPLIT_PAGE;
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
            newpage->AppendToSortedSet(curSep, pre->m_KeyLen, pre->m_Pointer.Read());
            spos++;
        }
        else
        {
            newpage->AppendToSortedSet(separator, sepLen, leftPage);
        }
    }
    // The entry in OldPos+1 points to the old page (the one that got split)
    // so we must set it to point to the corresponding new page
    newpage->GetKeyPtrPair(oldPos + 1)->m_Pointer = (char*)(rightPage);

    //_ASSERTE(newpage->FreeSpace() == 0);

    newPage = newpage;

exit:
    return btr;
}


BTRESULT BtreePage::SplitIndexPage(BtIterator* iter)
{
    _ASSERTE(IsIndexPage());

	//CloseLeafPage();

    BTRESULT btr = BT_SUCCESS;
    UINT lCount = m_nSortedSet / 2;
    UINT rCount = m_nSortedSet - lCount;

    // Copy first lCount records to the left new page (lower keys)
    UINT keySpace = 0;
    for (UINT i = 0; i < lCount; i++) keySpace += GetKeyPtrPair(i)->m_KeyLen;

    BtreePage* leftPage = nullptr;
	btr = m_Btree->AllocateIndexPage(lCount, keySpace, leftPage);
   if (btr != BT_SUCCESS) 
    { 
        goto exit; 
    }

    for (UINT i = 0; i < lCount; i++)
    {
        KeyPtrPair* pre = GetKeyPtrPair(i);
        char* sep = (char*)(this) + pre->m_KeyOffset;
		leftPage->AppendToSortedSet(sep, pre->m_KeyLen, pre->m_Pointer.Read());
    }
    //_ASSERTE(leftPage->FreeSpace() == 0);


    // Copy the higher rCount records into the right new page (higher keys)
    keySpace = 0;
    for (UINT i = lCount; i < m_nSortedSet; i++) keySpace += GetKeyPtrPair(i)->m_KeyLen;

    BtreePage* rightPage = nullptr;
	btr = m_Btree->AllocateIndexPage(rCount, keySpace, rightPage);
    if (btr != BT_SUCCESS) 
    { 
       goto exit; 
    }
 
    for (UINT i = lCount; i < m_nSortedSet; i++)
    {
        KeyPtrPair* pre = GetKeyPtrPair(i);
        char* sep = (char*)(this) + pre->m_KeyOffset;
		rightPage->AppendToSortedSet(sep, pre->m_KeyLen, pre->m_Pointer.Read());
    }
    //_ASSERTE(rightPage->FreeSpace() == 0);

     // Use the last key of the right page as separator for the two pages.
    // A separator thus indicates the highest key value allowed on a page.
    // The separator will be added to the parent index page.
    char* separator = (char*)(leftPage)+leftPage->GetKeyPtrPair(lCount - 1)->m_KeyOffset;
    UINT seplen = leftPage->GetKeyPtrPair(lCount - 1)->m_KeyLen;

#ifdef _DEBUG
	leftPage->m_SrcPage1 = this;
	rightPage->m_SrcPage1 = this;
	m_TrgtPage1 = leftPage;
	m_TrgtPage2 = rightPage;
#endif

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

BTRESULT BtreePage::TryToMergeIndexPage(BtIterator* iter)
{
    _ASSERTE(IsIndexPage());
  BTRESULT btr = BT_NO_MERGE;

  BtreePage* newPage = nullptr;

  UINT myCount = 0, myKeySpace = 0;
  UINT leftCount = 0, leftKeySpace = 0;
  UINT rightCount = 0, rightKeySpace = 0;

  BtreePage* parent = (BtreePage*)(iter->m_Path[iter->m_Count - 2].m_Page);
  UINT mySlot = iter->m_Path[iter->m_Count - 2].m_Slot;

  myCount = m_nSortedSet;
  myKeySpace = KeySpaceSize();

  BtreePage* leftPage = nullptr;
  BtreePage* rightPage = nullptr;

  if (mySlot > 0)
  {
	leftPage = parent->GetKeyPtrPair(mySlot - 1)->m_Pointer.ReadPP();
	leftCount = leftPage->m_nSortedSet;
	leftKeySpace = leftPage->KeySpaceSize();
  }
  if (mySlot + 1 < parent->SortedSetSize())
  {
	rightPage = parent->GetKeyPtrPair(mySlot + 1)->m_Pointer.ReadPP();
	rightCount = rightPage->m_nSortedSet;
	rightKeySpace = rightPage->KeySpaceSize();
  }

  UINT slotToDelete = 0;
  enum mergeType { NONE, LEFT, RIGHT } mergeDir;
  mergeDir = NONE;
  if (leftPage && rightPage)
  {
	// OK to merge either way.
	// Choose the "smaller" sibling - the one with less live space.
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
	  UINT pageSize = m_Btree->ComputeIndexPageSize(rightCount + myCount, rightKeySpace + myKeySpace);
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

  BtreePage* newParent = nullptr;
  if (newPage)
  {

	// Create new parent index page
	parent->ShrinkIndexPage(slotToDelete, newParent);
	KeyPtrPair* myNewEntry = newParent->GetKeyPtrPair(slotToDelete);
	myNewEntry->m_Pointer = (char*)(newPage);

	// Determine where to install the new page
	BtreePage** installAddr = nullptr;
	int parentIndx = iter->m_Count - 2;
	_ASSERTE(parentIndx >= 0);

	if (parentIndx == 0)
	{
	  // The b-tree object is the grandparent
	  installAddr = (BtreePage**)(&m_Btree->m_RootPage);
	}
	else
	{
	  BtreePage* grandParentPage = (BtreePage*)(iter->m_Path[parentIndx - 1].m_Page);
	  KeyPtrPair* recEntry = grandParentPage->GetKeyPtrPair(iter->m_Path[parentIndx - 1].m_Slot);
	  installAddr = (BtreePage**)(&recEntry->m_Pointer);
	}

	LONG64 oldval = InterlockedCompareExchange64((LONG64*)(installAddr), LONG64(newParent), LONG64(parent));
	if (oldval != LONG64(parent))
	{
	  btr = BT_INSTALL_FAILED;
	  goto exit;
	}
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
	m_Btree->m_nIndexPages--;
	newPage->m_Btree->m_MemoryBroker->Free(this, MemObjectType::IndexPage);
  }

  return btr;

}


BTRESULT BtreePage::MergeIndexPages(BtreePage* otherPage, bool mergeOnRight, BtreePage** newPage)
{

    _ASSERTE(IsIndexPage());
  BTRESULT btr = BT_SUCCESS;

  BtreePage* leftPage = nullptr;
  BtreePage* rightPage = nullptr;
  BtreePage* newIndexPage = nullptr;
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
  
  btr = m_Btree->AllocateIndexPage(recCount, keySpace, newIndexPage);
  if (btr != BT_SUCCESS)
  {
	goto exit;
  }
 

  // Insert the records into the new page
  for (UINT i = 0; i < leftPage->m_nSortedSet; i++)
  {
	KeyPtrPair* pre = leftPage->GetKeyPtrPair(i);
	char* key = (char*)(leftPage) + pre->m_KeyOffset;
	newIndexPage->AppendToSortedSet(key, pre->m_KeyLen, pre->m_Pointer.Read());
  }
  for (UINT i = 0; i < rightPage->m_nSortedSet; i++)
  {
	KeyPtrPair* pre = rightPage->GetKeyPtrPair(i);
	char* key = (char*)(rightPage) + pre->m_KeyOffset;
	newIndexPage->AppendToSortedSet(key, pre->m_KeyLen, pre->m_Pointer.Read());
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

BTRESULT BtreePage::ShrinkIndexPage(UINT dropPos, BtreePage*& newIndexPage)
{
  _ASSERTE(IsIndexPage());

  BTRESULT btr = BT_SUCCESS;
  BtreePage* newpage = nullptr;

  if (m_nSortedSet <= 1)
  {
	goto exit;
  }

  _ASSERTE(dropPos < m_nSortedSet);
  UINT sepLen = m_RecordArr[dropPos].m_KeyLen;

  btr = m_Btree->AllocateIndexPage(m_nSortedSet - 1, KeySpaceSize() - sepLen, newpage);
   if (btr != BT_SUCCESS)
  {
	goto exit;
  }

  if (newpage->m_PageSize < m_Btree->m_MinPageSize)
  {
	PageStatus* newpst = (PageStatus*)(&newpage->m_PageStatus);
	newpst->m_PendAction = PA_MERGE_PAGE;
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
	  newpage->AppendToSortedSet(curSep, pre->m_KeyLen, pre->m_Pointer.Read());
	}
	tpos++;
  }

  UINT frontSize = UINT((char*)(&newpage->m_RecordArr[m_nSortedSet-1]) - (char*)(newpage));
  UINT backSize = newpage->KeySpaceSize();
  _ASSERTE(frontSize + backSize == newpage->PageSize());

exit:
  newIndexPage = newpage;
  return btr;

}


void BtreePage::DeletePage(BtIterator* iter)
{
  // Check the index pages on the path up to the root looking for the first page that
  // conatins more then one record entry. This page will be updated and all index pages 
  // below it will be deleted because they will become empty. 
  const UINT MAXPAGES2DELETE = 10;
  BtreePage* pages2Delete[MAXPAGES2DELETE];
  UINT deleteCount = 0;

  BtreePage* parentPage = nullptr;
  int parentIndx = 0;
  for (parentIndx = iter->m_Count - 2; parentIndx >= 0; parentIndx--)
  {
	parentPage = (BtreePage*)(iter->m_Path[parentIndx].m_Page);
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
  BtreePage* newIndxPage = nullptr;
  BTRESULT hr = BT_SUCCESS;
  if (parentPage)
  {
	parentPage->ShrinkIndexPage(dropPos, newIndxPage);
  }
 
  // Determine where to install the new page
  BtreePage** installAddr = nullptr;
  if (parentIndx == -1)
  {
	// Deleting the last page of the tree so set then root pointer to null.
	_ASSERTE(newIndxPage == nullptr);
	installAddr = (BtreePage**)(&m_Btree->m_RootPage);
  }  else
  if (parentIndx == 0)
  {
	// The b-tree object is the grandparent
	installAddr = (BtreePage**)(&m_Btree->m_RootPage);
  }
  else
  {
	BtreePage* grandParentPage = (BtreePage*)(iter->m_Path[parentIndx-1].m_Page);
	KeyPtrPair* recEntry = grandParentPage->GetKeyPtrPair(iter->m_Path[parentIndx-1].m_Slot);
	installAddr = (BtreePage**)(&recEntry->m_Pointer);
  }
  *installAddr = newIndxPage;
  // TODO Make installation atomic...

  // Delete index pages that are no longer needed.
  for (UINT i = 0; i < deleteCount; i++)
  {
	BtreePage* pg = pages2Delete[i];
	m_Btree->m_EpochMgr->Deallocate(pg, MemObjectType::IndexPage);
    m_Btree->m_nIndexPages--;
  }
  // Finally delete the current leaf page
  m_Btree->m_nLeafPages--;
  m_Btree->m_EpochMgr->Deallocate(this, MemObjectType::IndexPage);


}

BTRESULT BtreeRootInternal::InstallNewPages(BtIterator* iter, BtreePage* leftPage, BtreePage* rightPage, 
                                            char* separator, UINT sepLen )
{
	UINT addedIndexPages = 0;

	BTRESULT btr = BT_SUCCESS;
	bool installed = false;
	BtreePage** installAddr = nullptr;
	BtreePage* expVal = nullptr;
		
	PathEntry* curpe = &iter->m_Path[iter->m_Count - 1];
	BtreePage* curPage = curpe->m_Page;

	PathEntry* parpe = nullptr;
	BtreePage* newParentPage = nullptr;
	BtreePage* parentPage = nullptr;
	LONGLONG*  pStatusAddr = nullptr;
	LONGLONG   parentsw = 0;

	PathEntry* gppe = nullptr;
	BtreePage* grandParentPage = nullptr;
	LONGLONG*  gpStatusAddr = nullptr;
	LONGLONG   gppsw = 0;

	// Install the two new pages
	if (iter->m_Count == 1)
	{
		// We are splitting the root page of the B-tree
		// B-tree object is the parent so update there
		newParentPage = CreateIndexPage(leftPage, rightPage, separator, sepLen);
		installAddr = (BtreePage**)(&(m_RootPage));
		expVal = curpe->m_Page;
		addedIndexPages++;
	}
	else
	{
		// The page has a previous parent page. Need to create a new parent page
		// that includes the two new pages and a seprator
		parpe = &iter->m_Path[iter->m_Count - 2];

		// Create a new instance of the parent index page that includes the new separator 
		// and the two new pages. Then update the pointer in the grandparent page.
		parentPage = parpe->m_Page;
		parentsw   = parpe->m_PageStatus;
		pStatusAddr = (LONGLONG*)(&parentPage->m_PageStatus);
		UINT oldPos = parpe->m_Slot;

		PageStatus* parentst = (PageStatus*)(&parentsw);
		_ASSERTE(parentst->m_PageState == PAGE_NORMAL);
		_ASSERTE(parentst->m_PendAction == PA_NONE);

		// The new parent page is a copy of the old parent page plus the new separator
		// Make sure that the old parent page doesn't change while we are copying over data

		btr = parentPage->ExpandIndexPage(separator, sepLen, leftPage, rightPage, oldPos, newParentPage);
		// Did some other thread update the parent page? If so, our new version may not
		// contain up-to-date information so don't install it.
		if (parentsw != parentPage->m_PageStatus.ReadLL())
		{
		  installed = false;
		  goto exit;
		}


		if (iter->m_Count == 2)
		{
			// The b-tree object is the grandparent
			installAddr = (BtreePage**)(&m_RootPage);
			expVal = parpe->m_Page;
 
		}
		else
		{
			gppe = &iter->m_Path[iter->m_Count - 3];
			grandParentPage = gppe->m_Page;
			gppsw = gppe->m_PageStatus;
			if (gppsw != grandParentPage->m_PageStatus.ReadLL())
			{
			  installed = false ;
			  goto exit;
			}
			gpStatusAddr = (LONGLONG*)(&grandParentPage->m_PageStatus);
			KeyPtrPair* recEntry = grandParentPage->GetKeyPtrPair(gppe->m_Slot);
			installAddr = (BtreePage**)(&recEntry->m_Pointer);
		}
		expVal = parentPage;
	}


	UINT64 mask = (UINT64)(1) << DescriptorFlagPos;
	MwCasDescriptorPool* pool = const_cast<MwCasDescriptorPool*>(GetDescriptorPool());
	MwCASDescriptor* desc = pool->AllocateMwCASDescriptor(mask);

	// We always have a new parent page so need to install it
	INT32 pos = desc->AddEntryToDescriptor((LONGLONG*)(installAddr), LONGLONG(expVal), LONGLONG(newParentPage));
	if (iter->m_Count > 1)
	{
	  // Page has a previous parent (index) page. Make the old parent inactive
	  pos = desc->AddEntryToDescriptor((LONGLONG*)(pStatusAddr), parentsw, PageStatus::MakePageInactive(parentsw));
	  if (iter->m_Count > 2)
	  {
		// Page has a grandparent (index) page - increment its update count (in the status word)
		pos = desc->AddEntryToDescriptor((LONGLONG*)(gpStatusAddr), gppsw, PageStatus::IncrUpdateCount(gppsw));
	  }
	}
	desc->CloseDescriptor();

	installed = desc->MwCAS(0);


exit:
	if (installed)
	{
	  // Success so delete the old parent page (if there was one)
	  if (parentPage) m_EpochMgr->Deallocate(parentPage, MemObjectType::IndexPage);
	  m_nIndexPages += addedIndexPages;
	  btr = BT_SUCCESS;
	}
	else
	{
	  // Failure so delete the new parent page
	  if( newParentPage) m_EpochMgr->DeallocateNow(newParentPage, MemObjectType::IndexPage);
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
    //btreeInt->CheckTree(stdout);
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

BTRESULT BtreeRoot::TraceRecord(KeyType* key)
{
  if (key == nullptr || key->m_pKeyValue == nullptr || key->m_KeyLen == 0)
  {
	return BT_INVALID_ARG;
  }
  BtreeRootInternal* btreeInt = (BtreeRootInternal*)(this);
  return btreeInt->TraceRecordInternal(key);
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
  m_FailList = nullptr;
}

void BtreeRootInternal::ClearTreeStats()
{
  m_nInserts = m_nDeletes = m_nUpdates = 0;
  m_nPageSplits = m_nConsolidations = m_nPageMerges = 0;
}

// Compute the page size to allocate
UINT BtreeRootInternal::ComputeLeafPageSize(UINT nrRecords, UINT keySpace, UINT minFree)
{
  UINT frontSpace = BtreePage::PageHeaderSize();
  UINT minKeySpace = sizeof(KeyPtrPair)*nrRecords+ keySpace;
  UINT freeSpace = max(minFree, UINT(minKeySpace*m_FreeSpaceFraction));
  
  // Leave room for two average size keys
  UINT twoKeys = UINT(2.0*(double(minKeySpace) / nrRecords));
  freeSpace = max(freeSpace, twoKeys);

  UINT pageSize = frontSpace + minKeySpace + freeSpace;
  return pageSize;
}

UINT BtreeRootInternal::ComputeIndexPageSize(UINT fanout, UINT keySpace)
{
  UINT frontSize = BtreePage::PageHeaderSize();
  INT  pageSize = frontSize + fanout * sizeof(KeyPtrPair) + keySpace;
  return pageSize;
}


// Find the path down to the target leaf page and store it in iter.
// The function does not include inactive pages in the path.
// However, when accessing the pages later on, their status may have changed.
//
BTRESULT BtreeRootInternal::FindTargetPage(KeyType* searchKey, BtIterator* iter)
{
	int attempts = 0;
	 LONGLONG psw = 0;
	 PageStatus* pst = (PageStatus*)(&psw);
	 BTRESULT btr = BT_SUCCESS;
	 iter->m_TrInfo = searchKey->m_TrInfo;

tryagain:
     btr = BT_SUCCESS;
     iter->m_Count = 0;

    // Descend down to the correct leaf page
    BtreePage* curPage = m_RootPage.ReadPP();
	int level = 0;
    while (curPage && curPage->IsIndexPage())
    {
		level++;
        psw = curPage->m_PageStatus.ReadLL();
		// Can't use an inactive page
		if (pst->m_PageState == PAGE_INACTIVE)
		{
		  goto tryagain;
		}

		int slot = curPage->KeySearch(searchKey, BtreePage::GTE);
		_ASSERTE(slot >= 0 && UINT(slot) < curPage->m_nSortedSet);
		iter->ExtendPath(curPage, slot, psw);

		// If the status of the page has changed,
		// the iterator info may be incorrect so try again
		if (psw != curPage->m_PageStatus.ReadLL())
		{
		  goto tryagain;
		}
       
		// Any pending actions on this page?
		if (pst->m_PendAction != PA_NONE)
		{
		  // Is it time to split this index page?
		  if (pst->m_PendAction == PA_SPLIT_PAGE)
		  {
			if (curPage->SplitIndexPage(iter) == BT_SUCCESS)
			{
			  m_nPageSplits++;
			}
			goto tryagain;
		  }

		  // Time to merge the page? But can't merge the root page because
		  // it has no siblings.
		  if (pst->m_PendAction == PA_MERGE_PAGE && iter->m_Count > 1)
		  {
			// Try to merge this page with its left or right neighbour
			if (curPage->TryToMergeIndexPage(iter) == BT_SUCCESS)
			{
			  m_nPageMerges++;
			}
			goto tryagain;
		  }
		}

		// Completed all pending actions, if any.
		// Now move to the next index level but not if the pointer is closed or
		// the status of the current index page has changed since we entered. 
		// (The last conditition is safe but perhaps overcautious. 
		BtreePage* nextPage = curPage->GetKeyPtrPair(slot)->m_Pointer.ReadPP();
		if (KeyPtrPair::IsClosedBitOn(ULONGLONG(nextPage)) || psw != curPage->m_PageStatus.ReadLL())
		{
		  goto tryagain;
		}
		curPage = nextPage;
    }

    if (curPage)
    {
       psw = curPage->m_PageStatus.ReadLL();

	   if (pst->m_PageState == PAGE_INACTIVE)
	   {
		 goto tryagain;
	   }

	   _ASSERTE(curPage->IsLeafPage());
	   iter->ExtendPath(curPage, -1, psw);

	 
		// Any action pending on thsi page
	   if (pst->m_PendAction != PA_NONE)
	   {
		 if (pst->m_PendAction == PA_CONSOLIDATE)
		 {
		   if (curPage->ConsolidateLeafPage(iter, 0) == BT_SUCCESS)
		   {
			 m_nConsolidations++;
		   }
		   goto tryagain;
		 }

		 // Is it time to split this leaf page?
		 if (pst->m_PendAction == PA_SPLIT_PAGE)
		 {
		   if (curPage->SplitLeafPage(iter) == BT_SUCCESS)
		   {
			 m_nPageSplits++;
		   }
		   goto tryagain;
		 }

		 if (pst->m_PendAction == PA_MERGE_PAGE && iter->m_Count > 1)
		 {
		   // Try to merge this page with its left or right neighbour
		   if (curPage->TryToMergeLeafPage(iter) == BT_SUCCESS)
		   {
			 m_nPageMerges++;
		   }
		   goto tryagain;
		 }
	   }

		// Don't trust the page if its status has changed
		// Perhaps too cautious but better safe than sorry
		if (psw != curPage->m_PageStatus.ReadLL())
		{
		  goto tryagain;
		}
    }
    return btr;
}


BTRESULT BtreeRootInternal::InsertRecordInternal(KeyType* key, void* recptr)
{
    LONGLONG epochId = 0;
    m_EpochMgr->EnterEpoch(&epochId);

tryagain:
     BTRESULT btr = BT_SUCCESS;
     BtIterator  iter(this);
 
    BtreePage* rootbase = m_RootPage.ReadPP();

    // Create a new root page if there isn't one already
    if (rootbase == nullptr)
    {
		BtreePage* page = nullptr;
		btr = AllocateLeafPage(1, key->m_KeyLen, page);
        if (btr != BT_SUCCESS)
        {
            goto exit;
        }
        LONG64 oldval = InterlockedCompareExchange64((LONG64*)(&m_RootPage), LONG64(page), LONG64(0));
        if (oldval != LONG64(0))
        {
            // Another thread already created the page so delete our version
            m_EpochMgr->DeallocateNow(page, MemObjectType::LeafPage);
        }
        else
        {
           m_nLeafPages++;
        }
        goto tryagain;
    }


    // Locate the target leaf page for the insertion
    btr = FindTargetPage(key, &iter);
    BtreePage* leafPage = (BtreePage*)(iter.m_Path[iter.m_Count-1].m_Page);
    _ASSERTE(leafPage && btr == BT_SUCCESS);
	PathEntry* ipe = (iter.m_Count > 1) ? &iter.m_Path[iter.m_Count - 2]: nullptr;
#ifdef DO_LOG
	InsertInfo::RecInsert('B', key->m_pKeyValue, key->m_KeyLen, leafPage, iter.m_Path[iter.m_Count-1].m_PageStatus, ipe);
#endif

	LONGLONG lpStatus = leafPage->m_PageStatus.ReadLL();
     btr = leafPage->AddRecordToPage(key, recptr);

	 if (btr == BT_SUCCESS) 
     {
#ifdef DISABLED
	   InsertInfo::RecInsert('E', key->m_pKeyValue, key->m_KeyLen, leafPage, lpStatus, ipe);
	   if (iter.m_Count > 1)
	   {
		 int cv = strncmp(key->m_pKeyValue, iter.m_Path[iter.m_Count - 2].m_Bound, key->m_KeyLen);
		 if (cv > 0)
		 {
		   FILE* LogFile = nullptr;
		   fopen_s(&LogFile, "LogFile", "w+");
		   if (LogFile == nullptr)
		   {
			 printf("Failed to open log file\n");
			 LogFile = stdout;
		   }

		   fprintf(LogFile, "****** Key %*s greater than bound %*s\n\n",
			 key->m_KeyLen, key->m_pKeyValue, iter.m_Path[iter.m_Count - 2].m_BoundLen, iter.m_Path[iter.m_Count - 2].m_Bound);
		   fprintf(LogFile, "Printing path down to leaf page\n");
		   for (UINT i = 0; i < iter.m_Count; i++)
		   {
			 PageStatus::PrintPageStatus(LogFile, iter.m_Path[i].m_PageStatus);
			 printf(" slot %d\n", iter.m_Path[i].m_Slot);
			 iter.m_Path[i].m_Page->PrintPage(stdout, 0, false);
		   }
		   leafPage->PrintLeafPage(LogFile, 0);

		   PrintLog(LogFile, 50000);
		   fprintf(LogFile, "======= end of log ===========\n");
		   key->m_TrInfo->Print(LogFile);
		   if (LogFile != stdout) fclose(LogFile);
		   printf("Dumpt finished\n");
		  }

	   }
#endif
         m_nRecords++;
		 m_nInserts++;
         goto exit; 
     }

	if (btr == BT_NOT_INSERTED)
	{
	  goto tryagain;
	}

    // Page is full so either enlarge and consolidate it or split it

	_ASSERTE(btr == BT_PAGE_FULL);
	LONGLONG psw = leafPage->m_PageStatus.ReadLL();
	PageStatus* pst = (PageStatus*)(&psw);
	LONGLONG newpsw = psw;
	PageStatus* newpst = (PageStatus*)(&newpsw);

	UINT recCount = 0, keySpace = 0;
	leafPage->LiveRecordSpace(recCount, keySpace);

	UINT newSize = (recCount + 1) * sizeof(KeyPtrPair) + keySpace + key->m_KeyLen;
	if (newSize < m_MaxPageSize)
	{
	  newpst->m_PendAction = PA_CONSOLIDATE;
	}
	else
	{
	  newpst->m_PendAction = PA_SPLIT_PAGE;
	}
	LONG64	rv = InterlockedCompareExchange64((LONGLONG*)(&leafPage->m_PageStatus), newpsw, psw);

	// Do the consolidation or split
	psw = leafPage->m_PageStatus.ReadLL();

	// Consolidate
	if (pst->m_PendAction == PA_CONSOLIDATE)
	{
	  btr = leafPage->ConsolidateLeafPage(&iter, key->m_KeyLen + sizeof(KeyPtrPair));
	  goto tryagain;
	}

     // Do split
	if (pst->m_PendAction == PA_SPLIT_PAGE)
	{
	  btr = leafPage->SplitLeafPage(&iter);
	  if (btr == BT_SUCCESS)
	  {
		m_EpochMgr->Deallocate(leafPage, MemObjectType::LeafPage);
		m_nPageSplits++;
	  }
	  goto tryagain;
	}
	goto tryagain;

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
    BtreePage* leafPage = (BtreePage*)(iter.m_Path[iter.m_Count - 1].m_Page);
    _ASSERTE(leafPage && btr == BT_SUCCESS);

    btr = leafPage->DeleteRecordFromPage(key);
    if (btr == BT_SUCCESS)
    {
	m_nRecords--;
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

tryagain:

    // Locate the target leaf page
    btr = FindTargetPage(key, &iter);
    if (btr != BT_SUCCESS)
    {
        goto exit;
    }
    BtreePage* leafPage = (BtreePage*)(iter.m_Path[iter.m_Count - 1].m_Page);
    _ASSERTE(leafPage);
    if (!leafPage)
    {
        btr = BT_INTERNAL_ERROR;
        goto exit;
    }

    // Found the target leaf page, now look for the record
    int pos = leafPage->KeySearch(key, BtreePage::EQ);
    if (pos < 0)
    {
        btr = BT_KEY_NOT_FOUND;
        goto exit;
    }

    KeyPtrPair* kpp = leafPage->GetKeyPtrPair(pos);
    _ASSERTE(kpp);
    recFound = kpp->m_Pointer.Read();

	if (KeyPtrPair::IsClosedBitOn(ULONGLONG(recFound)))
	{
	  goto tryagain;
	}

	LONGLONG psw = leafPage->m_PageStatus.ReadLL();
	PageStatus* pst = (PageStatus*)(&psw);

    if (pst->IsClosed())
    {
        goto tryagain;
    }

exit:
    m_EpochMgr->ExitEpoch(epochId);
    return btr;
}

BTRESULT BtreeRootInternal::TraceRecordInternal(KeyType* key)
{
  BTRESULT btr = BT_SUCCESS;
  BtIterator iter(this);

  LONGLONG epochId = 0;
  m_EpochMgr->EnterEpoch(&epochId);


  // Locate the target leaf page
  btr = FindTargetPage(key, &iter);
  if (btr != BT_SUCCESS)
  {
	goto exit;
  }
  BtreePage* leafPage = (BtreePage*)(iter.m_Path[iter.m_Count - 1].m_Page);
  _ASSERTE(leafPage);
  if (!leafPage)
  {
	btr = BT_INTERNAL_ERROR;
	goto exit;
  }

  // Found the target leaf page, now look for the record
  int pos = leafPage->KeySearch(key, BtreePage::EQ);
  if (pos >= 0)
  {
	printf("Record found in position %d\n ", pos);
	leafPage->PrintLeafPage(stdout, 0);
	goto exit;
  }
 
  printf("Record not found on leaf page\n");

  printf("========= Index pages =======\n\n");
  for (UINT i = 0; i < iter.m_Count - 1; i++)
  {
	BtreePage* page = iter.m_Path[i].m_Page;
	page->PrintPage(stdout, i + 1, false);
  }
  printf("========== End index pages ==============\n\n");

  BtreePage* curpage = leafPage;
  while (curpage != nullptr)
  {
	curpage->PrintLeafPage(stdout, 0);
	if (curpage->m_SrcPage1 != nullptr)
	{
	  printf("\n============Following Source page pointer =================\n\n");
	}
	curpage = curpage->m_SrcPage1;
  }

  printf("======== Checking failed pages  =====================\n");

  curpage = const_cast<BtreePage*>(m_FailList);
  while (curpage)
  {
	pos = curpage->KeySearch(key, BtreePage::EQ, true);
	curpage->PrintLeafPage(stdout, 0);

	//if (pos >= 0)
	//{
	//  curpage->PrintLeafPage(stdout, 0);
	//  break;
	//}
	curpage = curpage->m_SrcPage2;
  }
  printf("================= Checking done ================\n");

exit:
  m_EpochMgr->ExitEpoch(epochId);
  return btr;
}


void BtreeRootInternal::Print(FILE* file)
{
  BtreePage* rootPage = m_RootPage.ReadPP();
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
  BtreePage* rootPage = m_RootPage.ReadPP();
  if (rootPage)
  {
	if (rootPage->IsIndexPage())
	{
	  rootPage->ComputeTreeStats(statsp);
	}
	else
	{
	  rootPage->ComputeLeafStats(statsp);
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
				UINT(m_nRecords), UINT(m_nLeafPages), UINT(m_nIndexPages));
  fprintf(file, "Operations: %d inserts, %d deletes\n", UINT(m_nInserts), UINT(m_nDeletes));
  fprintf(file, "Page ops: %d consolidations, %d splits, %d merges\n", UINT(m_nConsolidations), UINT(m_nPageSplits), UINT(m_nPageMerges));

  fprintf(file, "Index pages\n");
  fprintf(file, "   Space: %d alloced, %d pages\n", stats.m_AllocedSpaceIP, stats.m_SpaceIP );
  fprintf(file, "   Space usage: %d headers, %d rec arrays, %d keys\n", stats.m_HeaderSpaceIP, stats.m_RecArrSpaceIP, stats.m_KeySpaceIP);

  fprintf(file, "Leaf pages\n");
  fprintf(file, "   %d live records\n", stats.m_Records);
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

    BtreePage* rootPage = m_RootPage.ReadPP();
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


BTRESULT BtreePage::AddRecordToPage(KeyType* key, void* recptr)
{
  LONGLONG psw = 0;
  PageStatus* pst = (PageStatus*)(&psw);
  BTRESULT btr = BT_SUCCESS;


tryagain:
  btr = BT_SUCCESS;
  psw = m_PageStatus.ReadLL();

  // Page has to be in normal state with no pending actions
  if (!(pst->m_PageState == PAGE_NORMAL && pst->m_PendAction == PA_NONE))
  {
	btr = BT_NOT_INSERTED;
	goto exit;
  }

  // And have enough free space for the new key/separator
  if (!EnoughFreeSpace(key->m_KeyLen, psw))
  {
	btr = BT_PAGE_FULL;
	goto exit;
  }
 
  // Reserve space for the new record by updating page status field.
  // OK to do so an interlocked operation even though its an MwCAS target field.
  LONGLONG newpsw = psw;
  PageStatus* newpst = (PageStatus*)(&newpsw);

  newpsw = psw;
  newpst->m_nUnsortedReserved++;
  newpst->m_LastFreeByte -= key->m_KeyLen;
  LONG64 oldval = InterlockedCompareExchange64((LONG64*)(&m_PageStatus), newpsw, psw);
  if (oldval != psw)
  {
	// No success, some other thread acquired that slot.
	goto tryagain;
  }

  // We've now reserved space so it's time to fill it in.
  // First copy the key into its reserved space
  char* keyBuffer = (char*)(this) + newpst->m_LastFreeByte + 1;
  memcpy(keyBuffer, key->m_pKeyValue, key->m_KeyLen);

  // Then fill in the slot in the record array
  UINT32 slotIndx = newpst->m_nUnsortedReserved - 1;
  KeyPtrPair* pentry = GetUnsortedEntry(slotIndx);
  pentry->m_KeyOffset = newpst->m_LastFreeByte + 1;
  pentry->m_KeyLen = key->m_KeyLen;
  MemoryBarrier();

  // Setting the record pointer makes the slot and record visible
  // Note that we count on the page having been zeroed on creating so we can
  // assume that m_Pointer is 0.
  ULONGLONG resVal = InterlockedCompareExchange64((LONGLONG*)(&pentry->m_Pointer), ULONGLONG(recptr), 0);
  if (resVal != 0)
  {
	// Some other thread sneaked in and closed the entry after we acquired the space 
	_ASSERTE(pentry->IsClosed());
	btr = BT_NOT_INSERTED;
  }
  key->m_TrInfo->m_HomePage = this;
  key->m_TrInfo->m_HomePos = slotIndx;

exit:
  return btr;

}

BTRESULT BtreePage::DeleteRecordFromPage(KeyType* key )
{
  _ASSERTE(IsLeafPage());

	LONGLONG psw = m_PageStatus.ReadLL();
	PageStatus* pst = (PageStatus*)(&psw);

    BTRESULT btr = BT_SUCCESS;
    if (pst->IsClosed())
    {
        btr = BT_PAGE_CLOSED;
        goto exit;
    }


    // Locate the record
    int pos = KeySearch(key, EQ);
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
        recPtr = ULONGLONG(kpp->m_Pointer);
        ULONGLONG retVal = 0;
        if (!KeyPtrPair::IsClosedBitOn(recPtr))
        {
            retVal = InterlockedCompareExchange64((LONGLONG*)(&kpp->m_Pointer), 0, recPtr);
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
	LONGLONG newpsw = 0;
	PageStatus* newpst = (PageStatus*)(&newpsw);

    if (btr == BT_SUCCESS)
    {
        // Update page status, that is, increment count of cleared slots
        while(true)
        {
            psw = m_PageStatus.ReadLL();
            if (pst->m_PageState != PAGE_NORMAL)
            {
                // No need to update the page status
                break;
            }
            newpsw = psw;
            newpst->m_SlotsCleared++;
            LONGLONG retVal = InterlockedCompareExchange64((LONGLONG*)(&m_PageStatus), newpsw, psw);
            if (retVal == psw)
            {
                // Success
                m_WastedSpace += sizeof(KeyPtrPair) + kpp->m_KeyLen;
                break;
            }

       }
    }
 

exit:
  return btr;

}


bool BtreePage::EnoughFreeSpace(UINT32 keylen, UINT64 pageState)
{
  LONGLONG psw = pageState;
  PageStatus* pst = (PageStatus*)(&psw);


  // First free byte (from beginning of the page
  UINT  slotsUsed = m_nSortedSet + pst->m_nUnsortedReserved;
  char* firstFree = (char*)(&m_RecordArr[slotsUsed]);

  char* lastFree = (char*)(this) + pst->m_LastFreeByte;

  INT64 freeSpace = lastFree - firstFree + 1;
  _ASSERTE(freeSpace >= 0);

  INT64 reqSpace = sizeof(KeyPtrPair) + keylen;

  return reqSpace <= freeSpace;
}

int _cdecl QSortCompareKeys(void* context, const void* leftp, const void* rightp)
{
  KeyPtrPair* lre = (KeyPtrPair*)(leftp);
  KeyPtrPair* rre = (KeyPtrPair*)(rightp);
  BtreePage* page = (BtreePage*)(context);

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


BTRESULT BtreePage::SortUnsortedSet(KeyPtrPair* pSortedArr, UINT count)
{
  qsort_s(pSortedArr, count, sizeof(KeyPtrPair), QSortCompareKeys, this);
  return (errno != 0) ? BT_INTERNAL_ERROR : BT_SUCCESS;
}

// Close the page and all its outgoing record pointers.
// Guaranteed to be true when the function exits
void BtreePage::CloseLeafPage()
{
	_ASSERTE(IsLeafPage());

	LONGLONG psw = 0;
	PageStatus* pst = (PageStatus*)(&psw);
	LONGLONG newpsw = 0;
	PageStatus* newpst = (PageStatus*)(&newpsw);
	LONG64 oldVal = 0;
	UINT slotCount = 0;

    while (true)
    {
        psw = m_PageStatus.ReadLL();

		switch( pst->m_PageState)
		{
		case PAGE_NORMAL:
		  // Try to set state to closing
		  newpsw = psw;
  		  newpst->m_PageState = PAGE_CLOSING;
		  oldVal = InterlockedCompareExchange64((LONGLONG*)(&m_PageStatus), newpsw, psw);
		  // Go try again
		  break;

		case PAGE_CLOSING:
		  slotCount = UINT(m_nSortedSet + pst->m_nUnsortedReserved);
		  for (UINT i = 0; i < slotCount; i++)
		  {
			  KeyPtrPair* kpp = GetKeyPtrPair(i);
			  kpp->CloseEntry(); 
		  }
		  newpsw = psw;
		  newpst->m_PageState = PAGE_CLOSED;
		  oldVal = InterlockedCompareExchange64((LONGLONG*)(&m_PageStatus), newpsw, psw);
		  // Fails if some other thread already closed the page
		  break;

		case PAGE_CLOSED:
		  newpsw = m_PageStatus.ReadLL();
		  _ASSERTE(pst->m_nUnsortedReserved == newpst->m_nUnsortedReserved);
           return;

		case PAGE_INACTIVE:
		  return;

		default:
		  _ASSERTE(false);
        }
    }

}

KeyPtrPair* BtreePage::GetKeyPtrPair(UINT indx)
{
  LONGLONG psw = m_PageStatus.ReadLL();
  PageStatus* pst = (PageStatus*)(&psw);

  KeyPtrPair* ptr = nullptr;
  if (indx < UINT(m_nSortedSet + pst->m_nUnsortedReserved))
  {
	ptr = &m_RecordArr[indx];
  }
  return ptr;
}

UINT BtreePage::FreeSpace()
{
  LONGLONG psw = m_PageStatus.ReadLL();
  PageStatus* pst = (PageStatus*)(&psw);

  UINT used = PageHeaderSize() + KeySpaceSize() + (m_nSortedSet + pst->m_nUnsortedReserved) * sizeof(KeyPtrPair);
  _ASSERTE(used <= m_PageSize);
  return UINT(m_PageSize - used);
}

UINT BtreePage::LiveRecordCount()
{
  LONGLONG psw = m_PageStatus.ReadLL();
  PageStatus* pst = (PageStatus*)(&psw);
  UINT count = m_nSortedSet + pst->m_nUnsortedReserved;
  if (m_PageType == LEAF_PAGE)
  {
	count -= pst->m_SlotsCleared;
  }
  return  count ;
}

// Get a consistent count of the number of live records and the key space they require
void BtreePage::LiveRecordSpace(UINT& recCount, UINT& keySpace)
{
  LONGLONG psw = 0;
  PageStatus* pst = (PageStatus*)(&psw);

  do
  {
    psw = m_PageStatus.ReadLL();

	if (IsIndexPage())
	{
	  recCount = m_nSortedSet;
	  keySpace = KeySpaceSize();
	}
	else
	{

	  // Count the number of records remaining on the page and the amount of space needed for the new page
	  recCount = 0;
	  keySpace = 0;
	  KeyPtrPair* pre = nullptr;
	  for (UINT i = 0; i < UINT(m_nSortedSet + pst->m_nUnsortedReserved) ; i++)
	  {
		  pre = GetKeyPtrPair(i);
		  if (pre->m_Pointer.Read())
		  {
			  recCount++;
			  keySpace += pre->m_KeyLen;
		  }
	  }
	}
	
	// Try again if page status has changed. 
  } while (psw != m_PageStatus.ReadLL());

}

BTRESULT BtreePage::ExtractLiveRecords(KeyPtrPair*& liveRecArray, UINT& count, UINT& keySpace)
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
	  liveRecArray[recCount].Set(pre->m_KeyOffset, pre->m_KeyLen, pre->m_Pointer.ReadPP());
	  recCount++;
      keySpace += pre->m_KeyLen;
    }
  }

  // Copy the unsorted record entries into an array and sort them
  //
  LONGLONG psw = m_PageStatus.ReadLL();
  PageStatus* pst = (PageStatus*)(&psw);
  KeyPtrPair* sortArr = nullptr;
  UINT nrUnsorted = pst->m_nUnsortedReserved;
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
	  if (pre->m_Pointer.Read() != nullptr)
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

BTRESULT BtreePage::CopyToNewPage(BtreePage* newPage)
{
  _ASSERTE(IsLeafPage());

  LONGLONG psw = m_PageStatus.ReadLL();
  PageStatus* pst = (PageStatus*)(&psw);

  KeyPtrPair* sortArr = nullptr;
  BTRESULT btr = BT_SUCCESS;
  
    // Copy the unsorted record entries into an array and sort them
  //
  UINT nrUnsorted = pst->m_nUnsortedReserved;
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
	  if (pre->m_Pointer.Read() != nullptr)
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
        if (pLeft->m_Pointer.Read() == nullptr)
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
	  newPage->AppendToSortedSet(lKey, pLeft->m_KeyLen, pLeft->m_Pointer.Read());
	  copiedRecs++;
	  copiedSpace += pLeft->m_KeyLen;

	  // Advance left input
            pLeft++;
            lCount--;
        }
        else
        {
            // Copy record entry and key value from left input
	  newPage->AppendToSortedSet(rKey, pRight->m_KeyLen, pRight->m_Pointer.Read());

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
	if (pLeft->m_Pointer.Read() == nullptr)
	{
	  pLeft++;
	  lCount--;
	  continue;
	}

        // Copy record entry and key value from left input
        char* lKey = (char*)(this) + pLeft->m_KeyOffset;
	newPage->AppendToSortedSet(lKey, pLeft->m_KeyLen, pLeft->m_Pointer.Read());
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
	newPage->AppendToSortedSet(rKey, pRight->m_KeyLen, pRight->m_Pointer.Read());
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
#ifdef _DEBUG
	newPage->m_SrcPage1 = this;
	m_TrgtPage1 = newPage;
#endif

exit:
  return btr;
}


BTRESULT BtreePage::ConsolidateLeafPage(BtIterator* iter, UINT minFree)
{

 tryagain:
    // Get a stable copy of the page status
	LONGLONG psw = m_PageStatus.ReadLL();
	PageStatus* pst = (PageStatus*)(&psw);

    BTRESULT btr = BT_SUCCESS;
    BtreePage* newPage = nullptr;

    UINT recCount = 0;
    UINT keySpace = 0;	
	LiveRecordSpace(recCount, keySpace);

    UINT newSize = m_Btree->ComputeLeafPageSize(recCount, keySpace, minFree);

    if (psw != m_PageStatus.ReadLL())
    {
        goto tryagain;
    }

	// Now go do the actual consolidation
    CloseLeafPage();
	psw = m_PageStatus.ReadLL();

#ifdef DO_LOG
	ConsInfo::RecCons('B', this, psw, nullptr, 0, recCount);
#endif

	// Recompute space requirements
	LiveRecordSpace(recCount, keySpace);
 
	btr = m_Btree->AllocateLeafPage(recCount, keySpace, newPage);
	if( btr != BT_SUCCESS)
	{
        goto exit;
    }

	btr = CopyToNewPage(newPage);
	_ASSERTE(newPage->m_nSortedSet == recCount);
	_ASSERTE(newPage->KeySpaceSize() == keySpace);
	
	if (psw != m_PageStatus.ReadLL())
	{
	  btr = BT_INSTALL_FAILED;
	  goto exit;
	}
	

    // Finally install the new page
	if (newPage)
    {
		_ASSERTE(LiveRecordCount() == newPage->LiveRecordCount());
		LONGLONG newpsw = newPage->m_PageStatus.ReadLL();
		PageStatus* newpst = (PageStatus*)(&newpsw);
		_ASSERTE(newpst->m_PageState == PAGE_NORMAL);
		_ASSERTE(newpst->m_PendAction == PA_NONE);
		_ASSERTE(newPage->m_nSortedSet == recCount);

        BtreePage** installAddr = nullptr;
		LONGLONG*   pStatusAddr = nullptr;
		LONGLONG	pStatusVal = 0;
		PageStatus* pst = (PageStatus*)(&pStatusVal);
		bool      installed = false;

        if (iter->m_Count == 1)
        {
            installAddr = (BtreePage**)(&m_Btree->m_RootPage);
        }
        else
        {
            BtreePage* parentPage = (BtreePage*)(iter->m_Path[iter->m_Count - 2].m_Page);
            KeyPtrPair* recEntry = parentPage->GetKeyPtrPair(iter->m_Path[iter->m_Count - 2].m_Slot);
            installAddr = (BtreePage**)(&recEntry->m_Pointer);
			pStatusAddr = (LONGLONG*)(&parentPage->m_PageStatus);
			pStatusVal = iter->m_Path[iter->m_Count - 2].m_PageStatus;
        }

		UINT64 mask = (UINT64)(1) << DescriptorFlagPos;
		MwCasDescriptorPool* pool = const_cast<MwCasDescriptorPool*>(GetDescriptorPool());
		MwCASDescriptor* desc = pool->AllocateMwCASDescriptor(mask);

		// Update record pointer
		INT32 pos = desc->AddEntryToDescriptor((LONGLONG*)(installAddr), LONGLONG(this), LONGLONG(newPage));

		if (pStatusAddr != nullptr)
		{
		  // Increment update counter of parent index page
		  pos = desc->AddEntryToDescriptor((LONGLONG*)(pStatusAddr), pStatusVal, PageStatus::IncrUpdateCount(pStatusVal));
		}

		// Make current leaf page inactive
		LONGLONG* curStatusAddr = (LONGLONG*)(&m_PageStatus);
		desc->AddEntryToDescriptor(curStatusAddr, psw, PageStatus::MakePageInactive(psw));

		desc->CloseDescriptor();

		installed = desc->MwCAS(0);
	
        if (!installed)
        {
            btr = BT_INSTALL_FAILED;
#ifdef NOTUSED
			printf("Thread %d: Install failed, p=0x%I64x, np=0x%I64x, cnt=%d, size=%d\n",
			        GetCurrentThreadId(), ULONGLONG(this), ULONGLONG(newPage), recCount, newPage->m_PageSize);
			while (true)
			{
			  BtreePage* expVal = const_cast<BtreePage*>(m_Btree->m_FailList);
			  newPage->m_SrcPage2 = expVal;
			  LONGLONG oldVal = InterlockedCompareExchange64((LONGLONG*)(&m_Btree->m_FailList), LONGLONG(newPage), LONGLONG(expVal));
			  if (oldVal == LONGLONG(expVal)) break;
			}
#endif
		}
#ifdef DO_LOG
		ConsInfo::RecCons('E', this, psw, newPage, 0, recCount);
#endif

	}

exit:
	iter->m_TrInfo->RecordAction(TraceInfo::CONS_PAGE, btr == BT_SUCCESS, this, newPage, nullptr);

    if (btr != BT_SUCCESS)
    {
        if (newPage) m_Btree->m_EpochMgr->Deallocate(newPage, MemObjectType::LeafPage);
        newPage = nullptr;
    }
	else
	{
	  m_Btree->m_nConsolidations++;
	  newPage->m_Btree->m_MemoryBroker->Free(this, MemObjectType::LeafPage);
	}
    return btr;
}


BTRESULT BtreePage::SplitLeafPage(BtIterator* iter)
{
 
  CloseLeafPage();

  BTRESULT btr = BT_SUCCESS;
  LONGLONG psw = m_PageStatus.ReadLL();
  PageStatus* pst = (PageStatus*)(&psw);

#ifdef DO_LOG
  SplitInfo::RecSplit('B', this, psw, nullptr, nullptr);
#endif

  // Copy a record entries for reccords that have not been deleted into a temporary array for sorting
  UINT nrSlots = m_nSortedSet + pst->m_nUnsortedReserved;
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
	if (pre && pre->m_Pointer.Read())
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

  BtreePage* leftPage = nullptr;
  btr = m_Btree->AllocateLeafPage(lCount, keySpace, leftPage);
  if (btr != BT_SUCCESS) 
  { 
      goto exit; 
  }

  for (UINT i = 0; i < lCount; i++)
  {
	char* key = (char*)(this) + sortArr[i].m_KeyOffset;
	leftPage->AppendToSortedSet(key, sortArr[i].m_KeyLen, sortArr[i].m_Pointer.Read());
  }

  // Copy the higher rCount records into the right new page (higher keys)
  keySpace = 0;
  for (UINT i = lCount; i < nrRecords; i++) keySpace += GetKeyPtrPair(i)->m_KeyLen;

  BtreePage* rightPage = nullptr;
  btr = m_Btree->AllocateLeafPage(rCount, keySpace, rightPage);
  if (btr != BT_SUCCESS) 
  { 
       goto exit; 
  }
 

  for (UINT i = lCount; i < nrRecords; i++)
  {
	char* key = (char*)(this) + sortArr[i].m_KeyOffset;
	rightPage->AppendToSortedSet( key, sortArr[i].m_KeyLen, sortArr[i].m_Pointer.Read());
  }
 
  // Use the last key of the left page as separator for the two pages.
  // A separator thus indicates the highest key value allowed on a page.
  // The separator will be added to the parent index page.
  char* separator = (char*)(leftPage)+leftPage->GetKeyPtrPair(lCount-1)->m_KeyOffset;
  UINT seplen = leftPage->GetKeyPtrPair(lCount-1)->m_KeyLen;
  _ASSERTE(LiveRecordCount() == leftPage->LiveRecordCount() + rightPage->LiveRecordCount());

#ifdef _DEBUG
  leftPage->m_SrcPage1 = this;
  rightPage->m_SrcPage1 = this;  
  m_TrgtPage1 = leftPage;
  m_TrgtPage2 = rightPage;
#endif

  // Install the new pages
  btr = m_Btree->InstallNewPages(iter, leftPage, rightPage, separator, seplen);

  iter->m_TrInfo->RecordAction(TraceInfo::SPLI_PAGE, btr == BT_SUCCESS, this, leftPage, rightPage);
#ifdef DO_LOG 
  SplitInfo::RecSplit('E', this, psw, leftPage, rightPage);
#endif
   if (btr == BT_INSTALL_FAILED)
  {
	leftPage->m_SrcPage2 = rightPage;
	while (true)
	{
	  BtreePage* expVal = const_cast<BtreePage*>(m_Btree->m_FailList);
	  rightPage->m_SrcPage2 = expVal;
	  LONGLONG oldVal = InterlockedCompareExchange64((LONGLONG*)(&m_Btree->m_FailList), LONGLONG(leftPage), LONGLONG(expVal));
	  if (oldVal == LONGLONG(expVal)) break;
	}
	m_Btree->m_EpochMgr->Deallocate(leftPage, MemObjectType::LeafPage);
	m_Btree->m_EpochMgr->Deallocate(rightPage, MemObjectType::LeafPage);
  }
  else
  {
	m_Btree->m_nPageSplits++;
	m_Btree->m_nLeafPages++;
  }

 exit:
   return btr;
 }

 BTRESULT BtreePage::TryToMergeLeafPage(BtIterator* iter)
  {
   BTRESULT btr = BT_SUCCESS;

   BtreePage* newPage = nullptr;

   UINT myCount = 0, myKeySpace = 0;
   UINT leftCount = 0, leftKeySpace = 0;
   UINT rightCount = 0, rightKeySpace = 0;

   BtreePage* parent = (BtreePage*)(iter->m_Path[iter->m_Count - 2].m_Page);
   UINT mySlot = iter->m_Path[iter->m_Count - 2].m_Slot;

   LiveRecordSpace(myCount, myKeySpace);

   BtreePage* leftPage = nullptr;
   BtreePage* rightPage = nullptr;

   if (mySlot > 0)
   {
	 leftPage = parent->GetKeyPtrPair(mySlot - 1)->m_Pointer.ReadPP();
	 leftPage->LiveRecordSpace(leftCount, leftKeySpace);
  }
   if (mySlot+1 < parent->SortedSetSize())
   {
	 rightPage = parent->GetKeyPtrPair(mySlot + 1)->m_Pointer.ReadPP();
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

  BtreePage* newParent = nullptr;
  if (newPage)
   {

	 // Create new parent index page

	 parent->ShrinkIndexPage(slotToDelete, newParent);
	 KeyPtrPair* myNewEntry = newParent->GetKeyPtrPair(slotToDelete);
	 myNewEntry->m_Pointer = (char*)(newPage);

	 // Determine where to install the new page
      BtreePage** installAddr = nullptr;
	 int parentIndx = iter->m_Count - 2;
	 _ASSERTE(parentIndx >= 0);

	 if (parentIndx == 0)
      {
          // The b-tree object is the grandparent
          installAddr = (BtreePage**)(&m_Btree->m_RootPage);
      }
      else
      {
	   BtreePage* grandParentPage = (BtreePage*)(iter->m_Path[parentIndx - 1].m_Page);
	   KeyPtrPair* recEntry = grandParentPage->GetKeyPtrPair(iter->m_Path[parentIndx - 1].m_Slot);
          installAddr = (BtreePage**)(&recEntry->m_Pointer);
      }

	 LONG64 oldval = InterlockedCompareExchange64((LONG64*)(installAddr), LONG64(newParent), LONG64(parent));
	 if (oldval != LONG64(parent))
	 {
	   btr = BT_INSTALL_FAILED;
	   goto exit;
	 }
	 m_Btree->m_nPageMerges++;
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
	 m_Btree->m_nLeafPages--;
	 newPage->m_Btree->m_MemoryBroker->Free(this, MemObjectType::LeafPage);
   }
   
   return btr;

 }

 // Merge two leaf pages: this and otherPage. 
 BTRESULT BtreePage::MergeLeafPages(BtreePage* otherPage, bool mergeOnRight, BtreePage** newPage)
 {
   _ASSERTE(IsLeafPage() && otherPage && otherPage->IsLeafPage());

   BTRESULT btr = BT_SUCCESS;

   KeyPtrPair* leftSortedArr = nullptr;
   char* leftBase = nullptr;
   UINT leftRecCount = 0;
   UINT leftKeySpace = 0;
   KeyPtrPair* rightSortedArr = nullptr;
   char* rightBase = nullptr;
   UINT rightRecCount = 0;
   UINT rightKeySpace = 0;

   BtreePage* newLeafPage = nullptr;
   *newPage = nullptr;

   // Close the two input pages
   CloseLeafPage();
   otherPage->CloseLeafPage();

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
   btr = m_Btree->AllocateLeafPage(leftRecCount + rightRecCount, keySpace, newLeafPage);
   if (btr != BT_SUCCESS)
   {
	 goto exit;
   }
 

   // Insert the records into the new page
   for (UINT i = 0; i < leftRecCount; i++)
   {
	 KeyPtrPair* pre = &leftSortedArr[i];
	 char* key = leftBase + pre->m_KeyOffset;
	 newLeafPage->AppendToSortedSet(key, pre->m_KeyLen, pre->m_Pointer.Read());
   }
   for (UINT i = 0; i < rightRecCount; i++)
   {
	 KeyPtrPair* pre = &rightSortedArr[i];
	 char* key = rightBase + pre->m_KeyOffset;
	 newLeafPage->AppendToSortedSet(key, pre->m_KeyLen, pre->m_Pointer.Read());
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
	// The shorter one is earlier in sort order
    if (keylen1 < keylen2) res = -1;
	else if (keylen1 > keylen2) res = 1;
  }
  return res;
}


#ifdef DO_LOG
volatile LONGLONG EventCounter = 0;
LogRec  EventLog[MaxLogEntries];


LogRec* NewEvent(char action, char BorE)
{
  UINT64 pos = InterlockedIncrement64(&EventCounter) - 1;
  LogRec* event = &EventLog[pos];
  event->m_Time = pos;
  event->m_ThreadId = GetCurrentThreadId();
  event->m_Action = action;
  event->m_BeginEnd = BorE;
  return event;
}

void InsertInfo::RecInsert(char BorE, char* key, UINT keylen, BtreePage* page, LONGLONG pstatus, PathEntry* prior)
{
  LogRec* pre = NewEvent('I', BorE);
  InsertInfo* pii = &pre->m_Insert;
  pii->m_Key = key;
  pii->m_KeyLen = keylen;
  pii->m_Page = page;
  pii->m_Status = pstatus;
  if (prior)
  {
	pii->m_Prior = *prior;
  }
}

void ConsInfo::RecCons(char BorE, BtreePage* trgt, LONGLONG status, BtreePage* newpage, LONGLONG newstatus, UINT reccount)
{
  LogRec* pre = NewEvent('C', BorE);
  ConsInfo* pci = &pre->m_Cons;
  pci->m_TrgPage = trgt;
  pci->m_TrgtStatus = status;
  pci->m_NewPage = newpage;
  pci->m_NewStatus = newstatus;
  pci->m_RecCount = reccount;
}

void SplitInfo::RecSplit(char BorE, BtreePage* srcpage, LONGLONG srcstate, BtreePage* left, BtreePage* right)
{
  LogRec* pre = NewEvent('S', BorE);
  SplitInfo* psi = &pre->m_Split;
  psi->m_SrcPage = srcpage;
  psi->m_SrcStatus = srcstate;
  psi->m_LeftPage = left;
  psi->m_RightPage = right;
}


void PrintLog(FILE* file, int eventCount)
{
  
  fprintf(file, "====== Event log =======\n");

  for (LONGLONG i = EventCounter - 1, cnt = 0; i >= 0, cnt < eventCount; i--,cnt++)
  {
	EventLog[i].Print(file);
  }
}
#endif DO_LOG
