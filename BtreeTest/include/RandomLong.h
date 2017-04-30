#include <windows.h>

#pragma once

class CRandomULongs {

  unsigned long x, y, z;	// Generator state variables

							// Very simple generator used only to set the initial
							// state (x,y,z) of the actual generator
  unsigned long simple_xor_rng(unsigned long rn) {
	unsigned long t = rn;
	t ^= (t << 13);
	t ^= (t >> 17);
	t ^= (t << 5);
	return(t);
  }

public:
  // Initialize or reset the state of the generator
  void Init(unsigned long seed)
  {
	// initialize with a random number if seed=0
	// (use lower half of high resolution counter)
	if (seed == 0) {
	  LARGE_INTEGER   counter;
	  QueryPerformanceCounter(&counter);
	  seed = counter.LowPart;
	}

	// Initialize the state variables (x,y,z)
	x = simple_xor_rng(seed);
	y = simple_xor_rng(x);
	z = simple_xor_rng(y);
  }

  // Constructor
  CRandomULongs(unsigned long seed = 0)
  {
	Init(seed);
  }

  // Return next random number
  unsigned long GetRandomULong() {
	unsigned long t;
	t = (x ^ (x << 3)) ^ (y ^ (y >> 19)) ^ (z ^ (z << 6));
	x = y;
	y = z;
	z = t;
	return(z);
  }
};

