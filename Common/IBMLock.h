#pragma once
#ifndef __COMMON_HTM_LOCK_H__
#define __COMMON_HTM_LOCK_H__

#include <htmxlintrin.h>
#include <cstring>
#include <atomic>
#include <iostream>
#include <SpinLock.h>

#define cpu_relax()     asm volatile ("" ::: "memory");

extern __inline long
__attribute__ ((__gnu_inline__, __always_inline__, __artificial__))
__TM_capacity_abort (void* const TM_buff)
{
  texasr_t texasr = *_TEXASR_PTR (TM_buff);
  return _TEXASR_FOOTPRINT_OVERFLOW (texasr);
}

extern __inline long
__attribute__ ((__gnu_inline__, __always_inline__, __artificial__))
__TM_conflict(void* const TM_buff)
{
  texasr_t texasr = *_TEXASR_PTR (TM_buff);
  /* Return TEXASR bits 11 (Self-Induced Conflict) through
     14 (Translation Invalidation Conflict).  */
  return (_TEXASR_EXTRACT_BITS (texasr, 14, 4)) ? 1 : 0;
}

extern __inline long
__attribute__ ((__gnu_inline__, __always_inline__, __artificial__))
__TM_user_abort (void* const TM_buff)
{
//  texasr_t texasr = *_TEXASR_PTR (TM_buff);
  texasr_t texasr = __builtin_get_texasr ();
  return _TEXASR_ABORT (texasr);
}

struct IBMLock{
	IBMLock(const size_t max_conflict_retries = 5, const size_t max_capacity_retries = 2){
		memset(&spinlock_, 0, sizeof(spinlock_));
		max_conflict_retries_ = max_conflict_retries;
		max_capacity_retries_ = max_capacity_retries;
#if defined(PROFILE_HTM)
		fallback_count_ = 0;
		total_count_ = 0;
		conflict_count_ = 0;
		capacity_count_ = 0;
		other_count_ = 0;
		explicit_count_ = 0;
#endif
	}

	// PROFILE_HTM collect statistics in the critical path, which will be the bottleneck for performance
	// should disable PROFILE_HTM when performance is perfered
	inline void Lock(){
#if defined(PROFILE_HTM)
		++total_count_;
#endif
		int capacity_abort_num = 0;
        	for (size_t i = 0; i < max_conflict_retries_; ++i){
			TM_buff_type TM_buff;
			while (spinlock_.IsLocked() == true){
        	                cpu_relax();
        	        }
			unsigned char tx_status = __TM_begin(&TM_buff);
			if (tx_status == _HTM_TBEGIN_STARTED) {
				if (spinlock_.IsLocked() == false){
        	                      return;
				}
				__TM_abort();
			}
#if defined(PROFILE_HTM)
			else if(__TM_conflict(&TM_buff)){
				conflict_count_++;
			}
			else if (__TM_user_abort(&TM_buff)) {
				explicit_count_++;
        	        }
#endif
			else if(__TM_capacity_abort(&TM_buff)){
#if defined(PROFILE_HTM)
				capacity_count_++;
#endif
				capacity_abort_num++;
                	        if (capacity_abort_num >= max_capacity_retries_){
                       		        break;
                        	}
			}
#if defined(PROFILE_HTM)
			else{
				other_count_++;
			}
#endif
		}
#if defined(PROFILE_HTM)
		++fallback_count_;
#endif
		spinlock_.Lock();
	}

	inline void Unlock(){
		if (spinlock_.IsLocked() == true){
			spinlock_.Unlock();
		}
		else{
			__TM_end();
		}
	}

	void Print(){
#if defined(PROFILE_HTM)
		printf("Total commits: %d\n\tHTM commits:  %d\n\tGL commits: %d\nTotal aborts: %d\n\tHTM conflict aborts:  %d\n\tHTM user aborts:  %d\n\tHTM capacity aborts:  %d\n\tHTM other aborts:  %d\n",
			int(total_count_),int(total_count_-fallback_count_), int(fallback_count_), int(conflict_count_+capacity_count_+explicit_count_+other_count_), int(conflict_count_), int(explicit_count_), int(capacity_count_), int(other_count_));

		printf("fallback rate=%f, conflict rate=%f, capacity rate=%f, explicit rate=%f, other rate=%f\n",
			fallback_count_ * 1.0 / total_count_,
			conflict_count_ * 1.0 / total_count_,
			capacity_count_ * 1.0 / total_count_,
			explicit_count_ * 1.0 / total_count_,
			other_count_ * 1.0 / total_count_
			);
#endif
	}
#if defined(PROFILE_HTM)
	std::atomic<size_t> fallback_count_;
	std::atomic<size_t> total_count_;
	std::atomic<size_t> conflict_count_;
	std::atomic<size_t> capacity_count_;
	std::atomic<size_t> other_count_;
	std::atomic<size_t> explicit_count_;
#endif
private:
	SpinLock spinlock_;
	size_t max_conflict_retries_;
	size_t max_capacity_retries_;
};

#endif
