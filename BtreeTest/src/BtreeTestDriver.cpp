#include <Windows.h>
#include "BtreeInternal.h"

const int SRC_KEYS = 25000;
const int MAX_KEYS = 100000;
const int KEYLENGTH = 32;
const int MAX_THREADS = 100;

UINT       csrckeys = 0;
int        nokeys = 0;
char       srctable[SRC_KEYS][KEYLENGTH + 1];
char       keytable[MAX_KEYS][KEYLENGTH + 1];
char      *keyptr[MAX_KEYS];

TraceInfo insertfb[SRC_KEYS];

MwCasDescriptorPool descPool(4, 8);


struct ThreadParams
{
    UINT        m_ThreadId;
    HANDLE      m_ThreadHandle;
    BtreeRoot*  m_Btree;

    UINT        m_Begin;
    UINT        m_Incr;
    UINT        m_Mod;
	UINT		m_Mult;

    UINT        m_Inserts;

    UINT        m_RecsInserted;
	UINT		m_RecsDeleted;

};

bool            RunFlag = false;
volatile LONG   threadsRunning = 0;


ThreadParams    paramArr[MAX_THREADS];

DWORD WINAPI ThreadFunction(void* p)
{
    ThreadParams* param = (ThreadParams*)(p);

    BTRESULT btr = BT_SUCCESS;
    KeyType searchKey;
    void*   recFound = nullptr;
    BtreeRoot* btree = param->m_Btree;

    UINT indx = param->m_Begin;
    UINT step = param->m_Incr;
    param->m_RecsInserted = 0;
	param->m_RecsDeleted = 0;

    INT64 spinCount = 0;
    while (RunFlag == false)
    {
        spinCount++;
    }

    for (UINT i = 0; i < param->m_Inserts; i++)
    {
	  UINT pos = indx*param->m_Mult + param->m_ThreadId;
        UINT keylen = UINT(strlen(srctable[pos]));
        searchKey.m_pKeyValue = srctable[pos];
        searchKey.m_KeyLen = keylen;

		searchKey.m_TrInfo = &insertfb[pos];

        btr = btree->InsertRecord(&searchKey, srctable[pos]);
		if (btr != BT_SUCCESS)
		{
		  printf("Thread %d, i=%d: Insertion failure, %s\n", GetCurrentThreadId(), i, searchKey.m_pKeyValue);
		  searchKey.m_TrInfo->Print(stdout);
		}
		searchKey.m_TrInfo->m_DoRecord = false;

        param->m_RecsInserted++;
		//btree->CheckTree(stdout);
        //btree->Print(stdout);
#ifdef _DEBUG
        btr = btree->LookupRecord(&searchKey, recFound);
        char *charKey = (char*)(recFound);
        if (btr != BT_SUCCESS || recFound != srctable[pos])
        {
            fprintf(stdout, "Thread %d, i=%d: Lookup failure, %s\n", GetCurrentThreadId(), i, searchKey.m_pKeyValue);
			searchKey.m_TrInfo->Print(stdout);
			fprintf(stdout, "\n");
		}

#endif
        indx = (indx + step) % param->m_Mod;
    }
    printf("Tread %d: %d inserts\n", param->m_ThreadId, param->m_RecsInserted);


	indx = param->m_Begin;
	step = param->m_Incr;
	
	for (UINT i = 0; i < param->m_Inserts; i++)
	{
	  UINT pos = indx*param->m_Mult + param->m_ThreadId;
	  UINT keylen = UINT(strlen(srctable[pos]));
	  searchKey.m_pKeyValue = srctable[pos];
	  searchKey.m_KeyLen = keylen;

	  searchKey.m_TrInfo = &insertfb[pos];

	  btr = btree->DeleteRecord(&searchKey);
	  if (btr != BT_SUCCESS)
	  {
		printf("Thread %d, i=%d: Delete failure, %s\n", GetCurrentThreadId(), i, searchKey.m_pKeyValue);
		searchKey.m_TrInfo->Print(stdout);
	  }
	  searchKey.m_TrInfo->m_DoRecord = false;

	  param->m_RecsDeleted++;
	  //btree->CheckTree(stdout);
	  //btree->Print(stdout);
#ifdef _DEBUG
	  btr = btree->LookupRecord(&searchKey, recFound);
	  char *charKey = (char*)(recFound);
	  if (btr == BT_SUCCESS)
	  {
		fprintf(stdout, "Thread %d, i=%d: Deleted record found, %s\n", GetCurrentThreadId(), i, searchKey.m_pKeyValue);
		searchKey.m_TrInfo->Print(stdout);
		fprintf(stdout, "\n");
	  }

#endif
	  indx = (indx + step) % param->m_Mod;
	}
	printf("Tread %d: %d deletes\n", param->m_ThreadId, param->m_RecsDeleted);


    InterlockedDecrement(&threadsRunning);


    return 0;
}

UINT            cthreads = 1;


int main()
{
  unsigned     seed = 12345;
  char         fname[100];
  FILE        *fp = nullptr;

  printf("\nTest driver for lock-free B-tree\n\n");
  //printf("max threads:     "); scanf_s("%d", &cthreads);
  //printf("random seed: ");     scanf_s("%d", &seed);
  printf("input file: ");      scanf_s("%s", fname, 100);
  errno_t err = fopen_s(&fp, fname, "r");
  if (err != 0 || !fp) {
	printf("Can't open file %s\n", fname);
	exit(1);
  }

  srand(seed);
  UINT maxlen = 0;
  while (fscanf_s(fp, "%s", &srctable[csrckeys][0], KEYLENGTH + 1) != EOF) {
	UINT len = UINT(strlen(&srctable[csrckeys][0]));
	maxlen = max(maxlen, len);
	csrckeys++;
	if (csrckeys >= SRC_KEYS) break;
  }
  fclose(fp);
  printf("loaded %d keys, max length %d \n", csrckeys, maxlen);

  BtreeRoot* btree = new BtreeRootInternal();

  SetDescriptorPool(&descPool);

  //csrckeys = 5000;

  int trange = csrckeys / cthreads;

  for (UINT i = 0; i < cthreads; i++)
  {
      ThreadParams* par = &paramArr[i];
      par->m_Btree = btree;
      par->m_ThreadId = i ;
      par->m_Incr = 7777 ;
      par->m_Begin = (13 * 13 + i) % trange;
      par->m_Mod = trange;
	  par->m_Mult = cthreads;
	  par->m_Inserts =  trange;
      par->m_RecsInserted = 0;
  }
  threadsRunning = cthreads;

  for (UINT i = 0; i < cthreads; i++)
  { 
     DWORD threadId = 0;
     ThreadParams* par = &paramArr[i];
     par->m_ThreadHandle = CreateThread(NULL, 0, ThreadFunction, par, 0, &threadId);
     _ASSERTE(par->m_ThreadHandle != NULL);
  }

  // Release threads to run
  RunFlag = true;

  while (threadsRunning > 0)
  {
      Sleep(1000);
  }

  //btree->Print(stdout);
  btree->CheckTree(stdout);
  btree->PrintStats(stdout);

 
  KeyType searchKey;
  char*  recordFound;
  UINT   missing = 0;
  for (UINT i = 0; i < csrckeys; i++)
  {
	UINT keylen = UINT(strlen(srctable[i]));
	searchKey.m_pKeyValue = srctable[i];
	searchKey.m_KeyLen = keylen;

	BTRESULT btr = btree->LookupRecord(&searchKey, (void*&)(recordFound));
	if (btr != S_OK || searchKey.m_pKeyValue != recordFound)
	{
	  fprintf(stdout, "i=%d: Record %*s not found\n", i, keylen, srctable[i]);
	  insertfb[i].Print(stdout);

	  btr = btree->LookupRecord(&searchKey, (void*&)(recordFound));
	  //btree->Print(stdout);
	  btree->TraceRecord(&searchKey);
	  missing++;
	}
  }	
  if (missing == 0)
  {
	fprintf(stdout, "No records missing from the tree\n");
  }

  return 0;

#ifdef NOTNOW
  indx = 5;
  for (UINT i = 0; i < csrckeys; i++)
  {
      UINT keylen = UINT(strlen(srctable[indx]));
      searchKey.m_pKeyValue = srctable[indx];
      searchKey.m_KeyLen = keylen;

      hr = btree->DeleteRecord(&searchKey);
      if (hr != S_OK)
      {
          fprintf(stdout, "Delete failed\n");
      }
#ifdef _DEBUG
	  //btree->CheckTree(stdout);
#endif
	  if (i > 0 && (i % 5000) == 0) btree->PrintStats(stdout);

      indx = (indx + step) % csrckeys;
  }
  btree->PrintStats(stdout);

  btree->Print(stdout);
#endif
}