#include<iostream>
#include<vector>
#include<map>
#include<fstream>
#include<math.h>
#include<string.h>
#include<stdlib.h>

using namespace std;

const char *_DELIM_TAB_ = "\t";
const char *_DELIM_2DOTS_ = "..";
const int _MAX_COL_ = 4;

struct refGenesRange {
	unsigned long long begin;
	unsigned long long end;
};

int main( int argc, char *argv[] ) {
	if( argc != 3 ) {
		cout << "Incorrect usage" << endl;
		cout << "Usage: " << argv[0] << " <ptt-file> <pileup-files-count>" << endl;
		return -1;
	}

	ifstream pttFile( argv[1] );
	if( !pttFile.is_open() ) {
		cout << "Error opening file" << endl;
		return -1;
	}

	vector<refGenesRange> refGenes;

	char *cLine;
	int pos;
	string range, line;
	refGenesRange rangeVal;

	while( !pttFile.eof() ) {
		getline( pttFile, line );
		if( "" == line )
			break;

		cLine = (char *)(line.c_str());
		range = strsep( &cLine, _DELIM_TAB_ );
		pos = range.find(_DELIM_2DOTS_);

		rangeVal.begin = atoll( range.substr( 0, pos ).c_str() );
		rangeVal.end = atoll( range.substr( pos + strlen( _DELIM_2DOTS_ ), range.length() ).c_str() );

		refGenes.push_back( rangeVal );
		//cout << "begin: " << rangeVal.begin << ", end: " << rangeVal.end << endl;
	}

	pttFile.close();

	int depth, i;
	int pileupFileCnt = atoi( argv[2] );
	long long index;
	map<long long, int> genomeDepthMapper;
	map<long long, int>::iterator it;

	float avgDepthCnt = 0;
	unsigned long long idx;
	float expMatrix[ refGenes.size() ][ pileupFileCnt ];
	//float expMatrix[ 100 ][ 1 ];

	for( int j = 1 ; j <= pileupFileCnt ; j++ ) {
		for( it = genomeDepthMapper.begin() ; it != genomeDepthMapper.end() ; it++ ) {
			genomeDepthMapper[ (*it).first ] = 0;
		}

		char pileupFileName[30];
		sprintf( pileupFileName, "%s%d%s", "t", j, ".pileup.out" );

		ifstream pileupFile( pileupFileName );
		if( !pileupFile.is_open() ) {
			cout << "Error opening file " << pileupFileName << endl;
			return -1;
		}

		while( !pileupFile.eof() ) {
			getline( pileupFile, line );
			if( "" == line )
				break;

			cLine = (char *)(line.c_str());

			i = 1;
			while( i <= _MAX_COL_ ) {
				string curToken = strsep( &cLine, _DELIM_TAB_ );

				switch( i ) {
					case 2:
						index = atoi( curToken.c_str() );
						break;
				
					case 4:
						depth = atoi( curToken.c_str() );
						break;
				}

				i++;
			}

			genomeDepthMapper[index] = depth;
		}

		pileupFile.close();

		for( int k = 0 ; k < refGenes.size() ; k++ ) {
			for( idx = refGenes[k].begin ; idx <= refGenes[k].end ; idx++ ) {
				if( 0 == genomeDepthMapper[idx] )
					avgDepthCnt += 0;
				else
					avgDepthCnt += genomeDepthMapper[idx];
			}

			avgDepthCnt /= ( refGenes[k].end - refGenes[k].begin );
			expMatrix[k][j] = avgDepthCnt;
		}
	}

	// Generate heapmap file
	string heatMapString = "heatMap.out";
	ofstream heatMapFile( heatMapString.c_str() );
	if( !heatMapFile.is_open() ) {
		cout << "Error opening file " << heatMapString << endl;
		return -1;
	}

	for( int k = 0 ; k < refGenes.size() ; k++ ) {
		for( int j = 0 ; j < pileupFileCnt ; j++ ) {
			if( j == (pileupFileCnt - 1) )
				heatMapFile << expMatrix[k][j];
			else
				heatMapFile << expMatrix[k][j] << " ";
		}

		heatMapFile << endl;
	}
	
	// Generate parallel coordinate
	string parCoString = "parCo.out";
	ofstream parCoFile( parCoString.c_str() );
	if( !parCoFile.is_open() ) {
		cout << "Error opening file " << parCoString << endl;
		return -1;
	}

	for( int j = 0 ; j < pileupFileCnt ; j++ ) {
		for( int k = 0 ; k < refGenes.size() ; k++ ) {
			if( k == (refGenes.size() - 1) )
				parCoFile << expMatrix[j][k];
			else
				parCoFile << expMatrix[j][k] << " ";
		}

		parCoFile << endl;
	}

	return 0;
}

