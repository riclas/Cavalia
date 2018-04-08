#pragma once
#ifndef __COMMON_HTM_LOCK_H__
#define __COMMON_HTM_LOCK_H__

#include <immintrin.h>
#include <cstring>
#include <atomic>
#include <iostream>
#include <SpinLock.h>

struct RtmLock{
	RtmLock(const size_t max_conflict_retries = 100, const size_t max_capacity_retries = 10){
		memset(&spinlock_, 0, sizeof(spinlock_));
		max_conflict_retries_ = max_conflict_retries;
		max_capacity_retries_ = max_capacity_retries;
#if defined(PROFILE_HTM)
		fallback_count_ = 0;
		total_count_ = 0;
		conflict_count_ = 0;
		nested_count_ = 0;
		capacity_count_ = 0;
		retry_count_ = 0;
		debug_count_ = 0;
		explicit_count_ = 0;
#endif
	}

	// PROFILE_HTM collect statistics in the critical path, which will be the bottleneck for performance
	// should disable PROFILE_HTM when performance is perfered
	inline void Lock(){
#if defined(PROFILE_HTM)
		++total_count_;
#endif
		unsigned status;
		int capacity_abort_num = 0;
		for (size_t i = 0; i < max_conflict_retries_; ++i){
/*			while (spinlock_.IsLocked() == true){
                                _mm_pause();
                        }*/
			status = _xbegin();
			if (status == _XBEGIN_STARTED){
				if (spinlock_.IsLocked() == false){
					return;
				}
				_xabort(0xff);
			}
			if ((status & _XABORT_EXPLICIT) && _XABORT_CODE(status) == 0xff && !(status & _XABORT_NESTED)){
				while (spinlock_.IsLocked() == true){
					_mm_pause();
				}
			}
			else if (status & _XABORT_CAPACITY){
				++capacity_abort_num;
				if (capacity_abort_num >= max_capacity_retries_){
					break;
				}
			}
			else if (!(status & _XABORT_RETRY)){
#if defined(PROFILE_HTM)
				++retry_count_;
#endif
				break;
			}
		}
#if defined(PROFILE_HTM)
		++fallback_count_;
		if(status & _XABORT_CONFLICT){
			++conflict_count_;
		}
		if(status & _XABORT_NESTED){
			++nested_count_;
		}
		if(status & _XABORT_CAPACITY){
			++capacity_count_;
		}
		if(status & _XABORT_DEBUG){
			++debug_count_;
		}
		if(status & _XABORT_EXPLICIT){
			++explicit_count_;
		}
#endif
		spinlock_.Lock();
	}

	inline void Unlock(){
		if (spinlock_.IsLocked() == true){
			spinlock_.Unlock();
		}
		else{
			_xend();
		}
	}

	void Print(){
#if defined(PROFILE_HTM)
		printf("lock count=%d, fallback count=%d, conflict_count=%d, nested_count=%d, capacity_count=%d, explicit_count=%d, retry_count=%d, debug_count=%d\n", int(total_count_), int(fallback_count_), int(conflict_count_), int(nested_count_), int(capacity_count_), int(explicit_count_), int(retry_count_), int(debug_count_));
		printf("fallback rate=%f, conflict rate=%f, nested rate=%f, capacity rate=%f, explicit rate=%f, retry rate=%f, debug rate=%f\n",
			fallback_count_ * 1.0 / total_count_,
			conflict_count_ * 1.0 / total_count_,
			nested_count_ * 1.0 / total_count_,
			capacity_count_ * 1.0 / total_count_,
			explicit_count_ * 1.0 / total_count_,
			retry_count_ * 1.0 / total_count_,
			debug_count_ * 1.0 / total_count_
			);
#endif
	}
#if defined(PROFILE_HTM)
	std::atomic<size_t> fallback_count_;
	std::atomic<size_t> total_count_;
	std::atomic<size_t> retry_count_;
	std::atomic<size_t> conflict_count_;
	std::atomic<size_t> nested_count_;
	std::atomic<size_t> capacity_count_;
	std::atomic<size_t> debug_count_;
	std::atomic<size_t> explicit_count_;
#endif
private:
	SpinLock spinlock_;
	size_t max_conflict_retries_;
	size_t max_capacity_retries_;
};

#endif
