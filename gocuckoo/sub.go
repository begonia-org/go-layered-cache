// Copyright 2024 geebytes. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package gocuckoo

// SubCF struct equivalent without bit fields
type SubCF struct {
	NumBuckets uint64 // This field was 56 bits in the C version
	BucketSize uint8  // This field was 8 bits in the C version, matches Go's uint8
	Data       []CuckooBucket
}

//	uint32_t SubCF_GetIndex(const SubCF *subCF, CuckooHash hash) {
//	    return (hash % subCF->numBuckets) * subCF->bucketSize;
//	}
//
// GetIndex returns the index of the bucket in the SubCF that corresponds to the given hash.
func (subCF *SubCF) GetIndex(hash CuckooHash) uint32 {
	return uint32((uint64(hash) % subCF.NumBuckets))
}

// Find returns true if the fingerprint is found in the SubCF, false otherwise.
// static int Filter_Find(const SubCF *filter, const LookupParams *params)
func (filter *SubCF) Find(params *LookupParams) bool {
	loc1 := filter.GetIndex(params.H1)
	loc2 := filter.GetIndex(params.H2)
	return (filter.NumBuckets > uint64(loc1) && filter.Data[loc1].Find(params.Fp) != -1) || (filter.NumBuckets > uint64(loc2) && filter.Data[loc2].Find(params.Fp) != -1)
}

// static uint8_t *Filter_FindAvailable(SubCF *filter, const LookupParams *params)
// FindAvailable returns a pointer to the first available slot in the SubCF, or nil if none are available.
// static uint8_t *Filter_FindAvailable(SubCF *filter, const LookupParams *params)
func (filter *SubCF) TryInsert(params *LookupParams) bool {
	loc1 := filter.GetIndex(params.H1)
	loc2 := filter.GetIndex(params.H2)
	ok := filter.Data[loc1].TryInsert(params.Fp)
	if ok {
		return ok
	}
	return filter.Data[loc2].TryInsert(params.Fp)
}

// Delete removes the fingerprint from the SubCF, returning true if the fingerprint was found.
// static int Filter_Delete(const SubCF *filter, const LookupParams *params)
func (filter *SubCF) Delete(params *LookupParams) bool {
	loc1 := filter.GetIndex(params.H1)
	loc2 := filter.GetIndex(params.H2)
	ok := filter.Data[loc1].Delete(params.Fp)
	if ok {
		return ok
	}
	return filter.Data[loc2].Delete(params.Fp)
}
