import re

def read_tickers(fname):
	res = set()
	with open(fname, 'r') as csv:
		for line in csv:
			x = re.sub(r'"', '', line.strip())
			res.add(x)

	return res

def write_file(tickers, fname):
	with open(fname, 'w') as csv:
		print('ticker', file=csv)
		for ticker in tickers:
			print(ticker, file=csv)

def pretty_diff(s1, s2):
	tickers = sorted([t for t in s1 - s2 if re.findall('^[A-Z]+$', t)])
	print(len(tickers), tickers)
	write_file(tickers, 'grouped_not_tickers.csv')

if __name__ == '__main__':
	grouped = read_tickers('agg1d_grouped.csv')
	aggs = read_tickers('agg1d_aggs.csv')
	tickers = read_tickers('tickers.csv')

	# pretty_diff(grouped, aggs)
	pretty_diff(aggs, grouped)
	# pretty_diff(grouped, tickers)
	# pretty_diff(ticker, grouped)
	# pretty_diff(aggs, tickers)
	# pretty_diff(tickers, aggs)
