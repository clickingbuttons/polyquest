import re

def read_tickers(fname):
	res = set()

	with open(fname, 'r') as csv:
		for line in csv:
			x = re.sub(r'"', '', line.strip())
			res.add(x)

	return res

def pretty_diff(s):
	return sorted([t for t in s ])

if __name__ == '__main__':
	grouped = read_tickers('jack_agg1d_grouped.csv')
	aggs = read_tickers('jack_agg1d_aggs.csv')
	tickers = read_tickers('tickers.csv')

	# print(len(grouped - aggs), pretty_diff(grouped - aggs))
	# print(len(aggs - grouped), pretty_diff(aggs - grouped))
	print(len(grouped - tickers), pretty_diff(grouped - tickers))
