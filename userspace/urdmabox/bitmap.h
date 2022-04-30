#ifndef BITMAP_H
#define BITMAP_H

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <math.h>

uint64_t bitmap_find_next_zero_area(uint8_t *bitmap, uint64_t bmap_size);

int bitmap_set(uint8_t *bitmap, uint64_t bitno);

int bitmap_clear(uint8_t *bitmap, uint64_t bitno);

#endif
