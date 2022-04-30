/*
 * RDMAbox
 * Copyright (c) 2021 Georgia Institute of Technology
 */

#include "bitmap.h"

uint64_t bitmap_find_next_zero_area(uint8_t *bitmap,uint64_t bmap_size) {
  uint64_t index = 0;
  uint64_t bit_index = 0;
  int val, found = 0;

  while(index < bmap_size) {
      bit_index = 0;
      while(bit_index < 8) {
      val = (int)pow(2, bit_index);

      if( !(bitmap[index] & val) ) {
	found = 1;
	break;
      }
      bit_index++;
    }

    if(!found)
      index++;
    else
      break;
  }

  if(found) {
    //printf("Returning with %lu\n", index);
    return (index * 8 + bit_index);
  }

  //printf("Returning not found!\n");
  return -1;
}

int bitmap_set(uint8_t *bitmap, uint64_t bitno) {
  //printf("%s called on bitno %lu\n", __func__, bitno);

  uint64_t index = bitno / 8;
  int bit_index = bitno % 8;

  int val = (int)pow(2, bit_index);
  bitmap[index] = bitmap[index] | (val);

  return 0;
}

int bitmap_clear(uint8_t *bitmap, uint64_t bitno) {
  //printf("%s called on bitno %lu\n", __func__, bitno);

  uint64_t index = bitno / 8;
  int bit_index = bitno % 8;

  int val = (int)pow(2, bit_index);
  bitmap[index] = bitmap[index] & ~(val);

  return 0;
}
