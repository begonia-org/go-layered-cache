// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package gocuckoo


// FindAvailable returns a pointer to the first available slot in the bucket, or nil if none are available.
// static uint8_t *Bucket_FindAvailable(CuckooBucket bucket, uint16_t bucketSize)
func (bucket CuckooBucket) TryInsert(fp CuckooFingerprint) bool {

	for index, value := range bucket {
		if value == 0 {
			bucket[index] = uint8(fp)
			return true
		}
	
	}
	return false
}

// static uint8_t *Bucket_Find(CuckooBucket bucket, uint16_t bucketSize, CuckooFingerprint fp)
// Find returns the index of the fingerprint in the bucket, or -1 if not found.
func (bucket CuckooBucket) Find(fp CuckooFingerprint) int {

	for index, value := range bucket {
		if value == uint8(fp) {
			return index
		}
	}
	return -1

}


// Delete removes the fingerprint from the bucket, returning true if the fingerprint was found.
// static int Bucket_Delete(CuckooBucket bucket, uint16_t bucketSize, CuckooFingerprint fp)
func (bucket CuckooBucket) Delete(fp CuckooFingerprint) bool {

	for index, value := range bucket {
		if value == uint8(fp) {
			bucket[index] = 0
			return true
		}
	}
	return false
}