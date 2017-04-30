#include <Windows.h>
#include "BtreeInternal.h"

char* rec1 = "record abc";
char* rec2 = "record bcde";
char* rec3 = "record abdef";

const int SRC_KEYS = 25000;
const int MAX_KEYS = 100000;
const int KEYLENGTH = 32;
const int MAX_THREADS = 100;

UINT       cthreads = 1;
UINT       csrckeys = 0;
int        nokeys = 0;
char       srctable[SRC_KEYS][KEYLENGTH + 1];
char       keytable[MAX_KEYS][KEYLENGTH + 1];
char      *keyptr[MAX_KEYS];

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

  HRESULT hr = S_OK;
  KeyType searchKey;
  void*   recFound = nullptr;

  UINT indx = 0;
  UINT step = 51;
  for (UINT i = 0; i < csrckeys; i++)
  {
	UINT keylen = UINT(strlen(srctable[indx]));
    searchKey.m_pKeyValue = srctable[indx];
    searchKey.m_KeyLen = keylen;

	hr = btree->InsertRecord(&searchKey, srctable[indx]);
	if (hr != S_OK)
	{
	  break;
	}

    hr = btree->LookupRecord(&searchKey, recFound);
    if (hr != S_OK || recFound != srctable[indx])
    {
        fprintf(stdout, "Lookup failure\n");
    }
    indx = (indx + step) % csrckeys;
  }

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

      indx = (indx + step) % csrckeys;
  }

  btree->Print(stdout);
}