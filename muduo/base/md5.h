//这个代码不是muduo库，来源于https://www.cnblogs.com/luxiaoxun/archive/2013/04/08/3008808.html
#ifndef MD5_H
#define MD5_H

#include <string>
#include <fstream>
#include <cstdint>
  
using std::string;
using std::ifstream;

/* MD5 declaration. */
class MD5 {
public:
	MD5();
	MD5(const void* input, size_t length);
	MD5(const string& str);
	MD5(ifstream& in);
	void update(const void* input, size_t length);
	void update(const string& str);
	void update(ifstream& in);
	const uint8_t* digest();
	string toString();
	void reset();

private:
	void update(const uint8_t* input, size_t length);
	void final();
	void transform(const uint8_t block[64]);
	void encode(const uint32_t* input, uint8_t* output, size_t length);
	void decode(const uint8_t* input, uint32_t* output, size_t length);
	string bytesToHexString(const uint8_t* input, size_t length);

	/* class uncopyable */
	MD5(const MD5&);
	MD5& operator=(const MD5&);

private:
	uint32_t _state[4];	/* state (ABCD) */
	uint32_t _count[2];	/* number of bits, modulo 2^64 (low-order word first) */
	uint8_t _buffer[64];	/* input buffer */
	uint8_t _digest[16];	/* message digest */
	bool _finished;		/* calculate finished ? */

	static const uint8_t PADDING[64];	/* padding for calculate */
	static const char HEX[16];
	enum { BUFFER_SIZE = 1024 };
};

#endif /*MD5_H*/
