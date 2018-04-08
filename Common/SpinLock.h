#pragma once
#ifndef __COMMON_SPIN_LOCK_H__
#define __COMMON_SPIN_LOCK_H__

#if defined(PTHREAD_LOCK) || defined(BUILTIN_LOCK)
#include <pthread.h>
#else
#include <boost/atomic.hpp>
#include <boost/smart_ptr/detail/spinlock.hpp>
#include <cstring>
#endif

#if defined(PTHREAD_LOCK)
struct SpinLock{
	SpinLock(){
		pthread_spin_init(&spinlock_, PTHREAD_PROCESS_PRIVATE);
	}

	inline void Lock(){
		pthread_spin_lock(&spinlock_);
	}

	inline void Unlock(){
		pthread_spin_unlock(&spinlock_);
	}

	inline bool IsLocked(){
		return *((volatile int*)(&spinlock_)) != 0;
	}

private:
	pthread_spinlock_t spinlock_;
};

#elif defined(BUILTIN_LOCK)
struct SpinLock{
	SpinLock(){
		spinlock_ = 0;
	}

	inline void Lock(){
		while(__sync_lock_test_and_set(&spinlock_, 1)){
			while(spinlock_);
		}
	}

	inline void Unlock(){
		__asm__ __volatile__("" ::: "memory");
		spinlock_ = 0;
	}

private:
	volatile bool spinlock_;
};

#else
struct SpinLock{
	SpinLock(){
		memset(&spinlock_, 0, sizeof(spinlock_));
	}

	inline void Lock(){
		spinlock_.lock();
	}

	inline void Unlock(){
		spinlock_.unlock();
	}

	inline bool IsLocked() const{
		return spinlock_.v_ == true;
	}

private:
	boost::detail::spinlock spinlock_;
};


#endif

#endif
