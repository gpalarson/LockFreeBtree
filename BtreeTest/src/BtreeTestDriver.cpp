#include <Windows.h>
#include"RandomLong.h"
#include "BtreeInternal.h"

const int SRC_KEYS = 25000;
const int MAX_KEYS = 1000000;
const int KEYLENGTH = 32;
const int MAX_THREADS = 100;

UINT       csrckeys = 0;
int        nokeys = 0;
char       srctable[SRC_KEYS][KEYLENGTH + 1];
char       keytable[MAX_KEYS][KEYLENGTH + 1];
char      *keyptr[MAX_KEYS];

TraceInfo insertfb[MAX_KEYS];


struct ThreadParams
{
    UINT        m_ThreadId;
    HANDLE      m_ThreadHandle;
    BtreeRoot*  m_Btree;

    UINT        m_RangeFirst;       // Firsta and lst index in keyptr array
    UINT        m_RangeLast;

    UINT        m_Inserts;

    UINT        m_RecsInserted;
	UINT		m_RecsDeleted;

};

bool            RunFlag = false;
volatile LONG   threadsRunning = 0;
FILE*           logFile = nullptr;


ThreadParams    paramArr[MAX_THREADS];

DWORD WINAPI ThreadFunction(void* p)
{
    ThreadParams* param = (ThreadParams*)(p);

    BTRESULT btr = BT_SUCCESS;
    KeyType searchKey;
    void*   recFound = nullptr;
    BtreeRoot* btree = param->m_Btree;

    param->m_RecsInserted = 0;
	param->m_RecsDeleted = 0;

    INT64 spinCount = 0;
    while (RunFlag == false)
    {
        spinCount++;
    }

    for (UINT i = param->m_RangeFirst; i <= param->m_RangeLast; i++)
    {
        searchKey.m_pKeyValue = keyptr[i];
        searchKey.m_KeyLen    = UINT(strlen(keyptr[i])) ;
		searchKey.m_TrInfo    = &insertfb[i];

        btr = btree->InsertRecord(&searchKey, keyptr[i]);
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
        if (btr != BT_SUCCESS || recFound != keyptr[i])
        {
            fprintf(stdout, "Thread %d, i=%d: Lookup failure, %s\n", GetCurrentThreadId(), i, searchKey.m_pKeyValue);
			searchKey.m_TrInfo->Print(stdout);
			fprintf(stdout, "\n");
#ifdef DO_LOG
            PrintLog(logFile, 20000);
#endif
            _exit(5);
		}

#endif
    }
    printf("Thread %d: %d inserts\n", param->m_ThreadId, param->m_RecsInserted);

    for (UINT i = param->m_RangeFirst; i <= param->m_RangeLast; i++)
	{
	  searchKey.m_pKeyValue = keyptr[i];
	  searchKey.m_KeyLen    = UINT(strlen(keyptr[i]));

	  searchKey.m_TrInfo = &insertfb[i];

 	  btr = btree->DeleteRecord(&searchKey);
      if (btr != BT_SUCCESS)
	  {
		printf("Thread %d, i=%d: Delete failure, btr=%d, %s\n", GetCurrentThreadId(), i, INT(btr), searchKey.m_pKeyValue);
		searchKey.m_TrInfo->Print(stdout);
#ifdef DO_LOG
		fprintf(logFile, "Thread %d, i=%d: Delete failure, btr=%d, %s\n", GetCurrentThreadId(), i, INT(btr), searchKey.m_pKeyValue);
        PrintLog(logFile, 5000);
        _exit(6);
#endif
	  }
	  searchKey.m_TrInfo->m_DoRecord = false;

	  param->m_RecsDeleted++;
	  //btree->CheckTree(stdout);
	  //btree->Print(stdout);
#ifdef NOT_NOW //_DEBUG
	  btr = btree->LookupRecord(&searchKey, recFound);
	  char *charKey = (char*)(recFound);
	  if (btr == BT_SUCCESS)
	  {
		fprintf(stdout, "Thread %d, i=%d: Deleted record found, %s\n", GetCurrentThreadId(), i, searchKey.m_pKeyValue);
		//searchKey.m_TrInfo->Print(stdout);
		//fprintf(stdout, "\n");
		//fprintf(logFile, "Thread %d, i=%d: Deleted record found, %s\n", GetCurrentThreadId(), i, searchKey.m_pKeyValue);
        //PrintLog(logFile, 20000);
        _exit(8);
      }

#endif
	}
	printf("Tread %d: %d deletes\n", param->m_ThreadId, param->m_RecsDeleted);


    InterlockedDecrement(&threadsRunning);


    return 0;
}

static int ExpandKeySet(int srckeys, int maxKeys)
{
    int totkeys = 0;
    int bound = min(MAX_KEYS, maxKeys);

    for (int k0 = 0; k0 < 25; k0++)
    {
        for (int k1 = 0; k1 < 25; k1++)
        {
            for (int i = 0; i < srckeys; i++) {
                if (totkeys >= bound) return(totkeys);

                keytable[totkeys][0] = '0' + k0;
                keytable[totkeys][1] = '0' + k1;
                memcpy(&keytable[totkeys][2], &srctable[i][0], KEYLENGTH - 1);
                keytable[totkeys][KEYLENGTH] = '\0';

                keyptr[totkeys] = &keytable[totkeys][0];
                totkeys++;
            }
        }
    }
    return totkeys;
}

static void ShuffleKeys(int seed, int nKeys)
{
    CRandomULongs rg(seed);
    char* tmp = nullptr;
    int indx = 0;
    
    // Shuffle the keys (in place) 
    for (int i = nKeys - 1; i > 0; i--)
    {
        indx = rg.GetRandomULong() % (i + 1);
        tmp = keyptr[i];
        keyptr[i] = keyptr[indx];
        keyptr[indx] = tmp;
    }
}

UINT            numThreads = 4;
int             keyCount = 1000000;

int main()
{
  unsigned     seed = 23456;
  char         fname[100];
  FILE        *fp = nullptr;

  printf("\nTest driver for lock-free B-tree\n\n");
  printf("no of threads:     "); scanf_s("%d", &numThreads);
  //printf("random seed: ");     scanf_s("%d", &seed);
  printf("input file: ");      scanf_s("%s", fname, 100);
  errno_t err = fopen_s(&fp, fname, "r");
  if (err != 0 || !fp) {
	printf("Can't open file %s\n", fname);
	exit(1);
  }

  err = fopen_s(&logFile, "LogFile.txt", "w");
  if (err != 0 || !logFile)
  {
      printf("Can't open log file\n");
      exit(2);
  }

  UINT maxlen = 0;
  while (fscanf_s(fp, "%s", &srctable[csrckeys][0], KEYLENGTH + 1) != EOF) {
	UINT len = UINT(strlen(&srctable[csrckeys][0]));
	maxlen = max(maxlen, len);
	csrckeys++;
	if (csrckeys >= SRC_KEYS) break;
  }
  fclose(fp);
  printf("Loaded %d keys, from '%s', max length %d \n", csrckeys, fname, maxlen);

  int numKeys = ExpandKeySet(csrckeys, keyCount);
  ShuffleKeys(seed, numKeys);
  printf("Expanded key set to %d keys and shuffled them\n", numKeys);


  BtreeRoot* btree = new BtreeRootInternal();

  int trange = numKeys / numThreads;

  for (UINT i = 0; i < numThreads; i++)
  {
      ThreadParams* par = &paramArr[i];
      par->m_Btree = btree;
      par->m_ThreadId = i ;
      par->m_RangeFirst = i*trange ;
      par->m_RangeLast = (i + 1)*trange - 1;
 	  par->m_Inserts =  trange;
      par->m_RecsInserted = 0;
  }
  threadsRunning = numThreads;

  for (UINT i = 0; i < numThreads; i++)
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

  btree->CheckTree(stdout);
  btree->PrintStats(stdout);
  //btree->Print(stdout);

  KeyType searchKey;
  char*  recordFound;
  UINT   missing = 0;
#ifdef _DEBUG
  for (int i = 0; i < numKeys; i++)
  {
	searchKey.m_pKeyValue = keyptr[i];
	searchKey.m_KeyLen = UINT(strlen(keyptr[i]));

	BTRESULT btr = btree->LookupRecord(&searchKey, (void*&)(recordFound));
	if (btr != BT_KEY_NOT_FOUND)
	{
	  fprintf(stdout, "i=%d: Record %s not found\n", i, searchKey.m_pKeyValue);
	  insertfb[i].Print(stdout);

	  btr = btree->LookupRecord(&searchKey, (void*&)(recordFound));
	  //btree->Print(stdout);
	  //btree->TraceRecord(&searchKey);
	  missing++;
	}
  }	
  if (missing == 0)
  {
	fprintf(stdout, "No records missing from the tree\n");
  }
#endif

  return 0;

}