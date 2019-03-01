#include <stdio.h>
#include <stdlib.h>


int main(void) {

	int j = 0;
	{
		int j = 55;
	}
	printf("%d", j);

}
