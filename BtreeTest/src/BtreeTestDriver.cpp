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

struct ThreadParams
{
    UINT        m_ThreadId;
    HANDLE      m_ThreadHandle;
    BtreeRoot*  m_Btree;

    UINT        m_Begin;
    UINT        m_Incr;
    UINT        m_Mod;

    UINT        m_Inserts;

    UINT        m_RecsInserted;

};

bool            RunFlag = false;
volatile LONG   threadsRunning = 0;
UINT            cthreads = 2;

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

    INT64 spinCount = 0;
    while (RunFlag == false)
    {
        spinCount++;
    }

    for (UINT i = 0; i < param->m_Inserts; i++)
    {
        UINT keylen = UINT(strlen(srctable[indx]));
        searchKey.m_pKeyValue = srctable[indx];
        searchKey.m_KeyLen = keylen;

        btr = btree->InsertRecord(&searchKey, srctable[indx]);
        if (btr != BT_SUCCESS)
        {
            printf("Thread %d: Insertion failure, %s\n", param->m_ThreadId, searchKey.m_pKeyValue);
         }
        param->m_RecsInserted++;
        //btree->Print(stdout);
#ifdef _DEBUG
        btr = btree->LookupRecord(&searchKey, recFound);
        char *charKey = (char*)(recFound);
        if (btr != BT_SUCCESS || recFound != srctable[indx])
        {
            fprintf(stdout, "Thread %d: Lookup failure, %s\n", param->m_ThreadId, searchKey.m_pKeyValue);
            //btree->CheckTree(stdout);
        }

#endif
        indx = (indx + step) % csrckeys;
    }

    InterlockedDecrement(&threadsRunning);

    printf("Tread %d: %d inserts\n", param->m_ThreadId, param->m_RecsInserted);

    return 0;
}

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
  printf("loaded %d keys, max length %d ", csrckeys, maxlen);

  BtreeRoot* btree = new BtreeRootInternal();

  for (UINT i = 0; i < cthreads; i++)
  {
      ThreadParams* par = &paramArr[i];
      par->m_Btree = btree;
      par->m_ThreadId = i + 1;
      par->m_Incr = 51;
      par->m_Begin = 13 * 13 + i;
      par->m_Mod = csrckeys;
      par->m_Inserts = 100; // csrckeys / cthreads;
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

  btree->Print(stdout);
  btree->CheckTree(stdout);
  

   btree->PrintStats(stdout);

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