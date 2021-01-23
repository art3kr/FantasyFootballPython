from bs4 import BeautifulSoup
import pandas as pd
import requests
from functools import reduce

'''weeks and positions to analyze'''
analyses = []
positions = ['QB',
# 'RB',
# 'WR',
# 'TE'
]

for week in range(12,17):

	for position in positions:

		analysis = [week, position]
		analyses.append(analysis) 

'''schemas'''
player_table_SCHEMA = [
'name',
'team',
'position',
'fpoints_half',
'games',
'player_url',
# 'fpoints_half_per_game'
]

schedule_SCHEMA = [
'week',
'team_1',
'team_2'
]

'''cutoffs'''
cutoffs = {
	'QB': [18,9],
	'RB': [99,99],
	'WR': [99,99],
	'TE': [99,99]
}


def get_schedule():
	'''function to get the table of all games scheduled from pro football reference

	input: none
	output: pandas dataframe
	'''

	'''send request, get html table of game schedule'''
	base_url = 'https://www.pro-football-reference.com/years/2020/games.htm'
	r = requests.get(base_url)
	soup = BeautifulSoup(r.content, 'html.parser')
	schedule_table_html = soup.find_all('table')[0]

	'''create lists'''
	weeks = []
	team_1s = []
	team_2s = []

	'''iterate through html table of players'''
	for index, row in enumerate(schedule_table_html.find_all('tr')[2:]):


		try: 
			'''get teams and weeks'''
			week = int(row.find('th', attrs={'data-stat': 'week_num'}).get_text())
			team_1 = row.find('td', attrs={'data-stat': 'winner'}).get_text()
			team_2 = row.find('td', attrs={'data-stat': 'loser'}).get_text()

			'''append to lists'''
			weeks.append(week)
			team_1s.append(team_1)
			team_2s.append(team_2)


		except:
			pass

	'''create dataframe from lists'''
	schedule_df = pd.DataFrame(
		list(zip(weeks,team_1s,team_2s)),
		columns=schedule_SCHEMA)

	return schedule_df



def get_all_players_table():
	'''function to get the table of all players fantasy points, games played, etc. from pro football reference

	input: none
	output: pandas dataframe
	'''

	'''send request, get html table of players'''
	base_url = 'https://www.pro-football-reference.com/years/2020/fantasy.htm'
	r = requests.get(base_url)
	soup = BeautifulSoup(r.content, 'html.parser')
	players_table_html = soup.find_all('table')[0]

	'''create lists'''
	names = []
	player_urls = []
	teams = []
	positions = []
	fpoints_half = []
	games = []

	'''iterate through html table of players'''
	for index, row in enumerate(players_table_html.find_all('tr')[2:]):

		try:
			'''get name, team, player url, position, fpoints and games played'''
			dat = row.find('td', attrs={'data-stat': 'player'})
			name = dat.a.get_text()
			player_url = dat.a.get('href')
			# team = row.find('td', attrs={'data-stat': 'team'}).get_text()
			team = row.find('td', attrs={'data-stat': 'team'}).a.get('title')
			position = row.find('td', attrs={'data-stat': 'fantasy_pos'}).get_text()
			fpoint_half = float(row.find('td', attrs={'data-stat': 'fanduel_points'}).get_text())
			game = int(row.find('td', attrs={'data-stat': 'g'}).get_text())


			'''append to lists'''
			names.append(name)
			player_urls.append(player_url)
			teams.append(team)
			positions.append(position)
			fpoints_half.append(fpoint_half)
			games.append(game)


		except: 
			pass

	'''create dataframe from lists'''
	players_df = pd.DataFrame(
		list(zip(names,teams,positions,fpoints_half,games,player_urls)),
		columns=player_table_SCHEMA)

	'''creating points per game stat'''
	players_df['fpoints_half_per_game'] = players_df['fpoints_half'] / players_df['games']

	return players_df


def get_points_allowed():

	'''function to get the table of teams and their qb points allowed from pro football reference

	input: none
	output: pandas dataframe
	'''

	'''send request, get html table of teams and points'''
	base_url = 'https://www.pro-football-reference.com/years/2020/fantasy-points-against-'
	positions = ['QB','RB','WR','TE']
	position_dfs = []

	'''iterate through position tables'''
	for position in positions:

		r = requests.get(base_url + position + '.htm')
		soup = BeautifulSoup(r.content, 'html.parser')
		points_allowed_table_html = soup.find_all('table')[0]

		'''create lists'''
		teams = []
		points_allowed_half = []
		
		'''iterate through html table of players'''
		for index, row in enumerate(points_allowed_table_html.find_all('tr')[2:]):

			try:
				'''get team and points allowed'''
				team = row.find('th', attrs={'data-stat': 'team'}).get_text()
				point_allowed_half = float(row.find('td', attrs={'data-stat': 'fanduel_points_per_game'}).get_text())

				'''append to lists'''
				teams.append(team)
				points_allowed_half.append(point_allowed_half)

			except:
				pass

		'''create dataframe from lists'''
		points_allowed_df = pd.DataFrame(
			list(zip(teams,points_allowed_half)),
			columns=['team','{}_points_allowed_half'.format(position)])

		'''append dataframe to list of position dfs'''
		position_dfs.append(points_allowed_df)

	'''concat all position points allowed dfs'''
	points_allowed_df = reduce(lambda left,right: pd.merge(left,right,on=['team'],how='outer'),position_dfs)

	return points_allowed_df

def analyze_week_matchup(analysis, schedule, players, points_allowed, cutoffs):

	'''getting just the matchup for this week'''
	matchup = schedule[schedule['week'] == analysis[0]]
	matchup_reverse = matchup.copy().rename(columns={'team_1':'team_2','team_2':'team_1'})
	matchup_double = pd.concat([matchup,matchup_reverse])

	'''merging with points allowed at position of interest'''
	position_points_allowed = points_allowed[['team','{}_points_allowed_half'.format(analysis[1])]]
	final_df = matchup_double.merge(position_points_allowed,left_on=['team_1'],right_on=['team'],how='outer').sort_values(by='{}_points_allowed_half'.format(analysis[1]),ascending=False)

	'''getting players at position of interest'''
	position_players = players[players['position'] == analysis[1]]
	position_players = position_players[['team','name','fpoints_half_per_game']]

	'''merging matchup and points allowed with players at position of interest'''
	final_df = final_df.merge(position_players,left_on=['team_2'],right_on=['team'],how='outer')
	final_df = final_df.drop(['team_x','team_y'],axis=1).rename(columns={'name':'team_2_player'})

	'''some trimming'''
	final_df = final_df[final_df[f'{analysis[1]}_points_allowed_half'] >= cutoffs[analysis[1]][0]]
	final_df = final_df[final_df['fpoints_half_per_game'] >= cutoffs[analysis[1]][1]]

	return final_df

def find_best_players():
	'''function to find best players each week at one position from 

	input: pandas dataframe
	output: pandas dataframe
	'''
	for week_df in position_dfs:
		best_week = week_df.head(15)
		


def main():
	schedule = get_schedule()
	players = get_all_players_table()
	points_allowed = get_points_allowed()

	final_dfs = {}

	for analysis in analyses:

		final = analyze_week_matchup(analysis, schedule, players, points_allowed, cutoffs)
		final_dfs[f'{analysis[1]}_{analysis[0]}'] = final
		final.to_csv(f'week_{analysis[0]}_{analysis[1]}s.csv')

	print(final_dfs.items())
	


if __name__ == "__main__":
	main()