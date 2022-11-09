from re import X
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
from pylab import *
import pandas as pd
import math

index_display = {
	"alex":"ALEX",
	"pgm":"PGM-Index",
	"xindex":"XIndex",
	"lipp":"LIPP",
	"alexol":"ALEX+",
	"hot":"HOT",
	"hotrowex":"HOT-ROWEX",
	"btree":"B+tree",
	"artolc":"ART-OLC",
	"artunsync":"ART",
	"btreeolc":"B+treeOLC",
	"artrowex":"ART-ROWEX",
	"wormhole_u64": "Wormhole",
	"masstree": "Masstree",
	"lippol":"LIPP+",
	"finedex": "FINEdex",
}

index_abbreviation = {"alex":"A", "pgm":"P", "xindex":"X", "hot":"h", "hotrowex":"h+", "wormhole_u64":"w", "btree":"b", "artolc":"r+", "art":"a", "btreeolc":"b+","alexol":"A+", "lipp":"L", "lippolc":"L++", "lippol":"L+++", "lipppt":"L+", "artunsync":"r", "masstree":"m", "tied":"T", "finedex":"F"}

dataset_abbreviation = {
	"biology_200M_uint64" : "genome",
	"books_200M_uint64" : "books",
	"covid_tweets_200M_uint64" : "covid",
	"eth_gas_27M_uint64": "eth",
	"fb_200M_uint64" : "fb",
	"gnomad_200M_uint64" : "gnomad",
	"libraries_io_repository_dependencies_200M_uint64" : "libio",
	"osm_cellids_200M_uint64" : "osm",
	"planet_features_osm_id_200M_uint64" : "planet",
	"osm_history_node_200M_uint64" : "history",
	"stovf_vote_id_200M_uint64" : "stack",
	"wise_all_sky_data_htm_200M_uint64" : "wise",
	"covid_osm_sd_200M_uint64": "covid->osm",
	"covid_biology_sd_200M_uint64": "covid->genome",
	"osm_covid_sd_200M_uint64": "osm->covid",
	"biology_covid_sd_200M_uint64": "genome->covid",
	"eth_cumgas_200M_uint64": "eth_gas",
	"wiki_revid_200M_uint64": "wiki_rev",
	"planetways_200M_uint64": "planetways",
	"wiki_ts_200M_uint64": "wiki_ts",
}

tab_colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:brown', 'tab:purple', 'tab:pink', 'tab:gray', 'tab:olive', 'tab:cyan']
markers = ['o', '+', 'X', 'p', '*', 'x', '1', '2', '3', '4']
hatches = ['//',  '\\\\', '++', 'xx', 'oo', 'OO', '..', '**']

dataset_abbreviation_rev = {v: k for k, v in dataset_abbreviation.items()}

g_e_local = 32
g_e_global = 4096

# Set the axle scale
# for scientific notation
class OOMFormatter(matplotlib.ticker.ScalarFormatter):
	def __init__(self, order=0, fformat="%1.0f", offset=True, mathText=False):
		self.oom = order
		self.fformat = fformat
		matplotlib.ticker.ScalarFormatter.__init__(self,useOffset=offset,useMathText=mathText)
	def _set_order_of_magnitude(self):
		self.orderOfMagnitude = self.oom
	def _set_format(self, vmin=None, vmax=None):
		self.format = self.fformat
		if self._useMathText:
			self.format = r'$\mathdefault{%s}$' % self.format

def read_data(path):
	data = pd.read_csv(path)
	data.rename(columns={'key_path': 'dataset'}, inplace=True)
	# data["dataset"] = data["dataset"].apply(lambda ds: ds.split("/")[-1]).apply(lambda ds: ds.split("_")[0]).apply(lambda ds: ds.split(".")[0])
	data["dataset"] = data["dataset"].apply(lambda ds: ds.split("/")[-1])
	data["dataset"] = data["dataset"].apply(lambda ds: ds[:-1] if ds[-1] == '|' else ds)
	data["dataset"] = data["dataset"].apply(lambda ds: "fb_200M_uint64" if ds == "fbpgm_200M_uint64" else ds)
	return data

def extract_heatmap_data(all_data, index, attr1="read_ratio", attr2="pgm", attr3="throughput", comparator="gt", use_abbrev = False, aggre="mean"):
	"""
	Helper function to get heatmap data from pandas dataset.
	"""
	# Initialize
	attr1_data = all_data[attr1].unique() # y-axis
	attr2_data = all_data[attr2].unique() # x-axis
	heatmap_mat =  np.zeros((len(attr1_data), len(attr2_data))) # values

	if comparator == "lt":
		heatmap_mat += 1e15

	index_mat = [["xxxxxxxxxxxxxx" for j in range(len(attr2_data))] for i in range(len(attr1_data))] # the index label for every point
	index_mat = np.asarray(index_mat)
	index_mat[:] = ""

	# find the max/min value from all index
	for idx in index:
		data = all_data[all_data["index_type"]==idx]
		for i in range(len(attr1_data)):
			for j in range(len(attr2_data)):
				if aggre == "50percent":
					attr3_data = np.percentile(data[attr3][data[attr1]==attr1_data[i]][data[attr2]==attr2_data[j]].array,50)
				elif aggre == "mean":
					attr3_data = np.mean(data[(data[attr1]==attr1_data[i]) & (data[attr2]==attr2_data[j])][attr3].array)
				elif aggre == "min":
					attr3_data = np.min(data[attr3][data[attr1]==attr1_data[i]][data[attr2]==attr2_data[j]].array)
				elif aggre == "max":
					attr3_data = np.max(data[attr3][data[attr1]==attr1_data[i]][data[attr2]==attr2_data[j]].array)
				if comparator == "gt":
					if attr3_data > heatmap_mat[i,j]:
						heatmap_mat[i,j] = attr3_data
						if use_abbrev and idx in index_abbreviation:
							index_mat[i,j] = index_abbreviation[idx]
						else:
							index_mat[i,j] = idx
				elif comparator == "lt":
					if attr3_data < heatmap_mat[i,j]:
						heatmap_mat[i,j] = attr3_data
						if use_abbrev and idx in index_abbreviation:
							index_mat[i,j] = index_abbreviation[idx]
						else:
							index_mat[i,j] = idx
	return attr1_data, attr2_data, heatmap_mat, index_mat

def get_index_area(mat):
	"""
	Helper function to label the connecting area with the same index.
	"""
	edges = np.zeros_like(mat, dtype=int)
	set_mat = np.zeros_like(mat, dtype=int)
	def check(mat, i, j):
		res = 0
		if i == 0 or mat[i-1,j] != mat[i,j]:
			res += 1
		if i == mat.shape[0]-1 or mat[i+1,j] != mat[i,j]:
			res += 4
		if j == 0 or mat[i,j-1] != mat[i,j]:
			res += 8
		if j == mat.shape[1]-1 or mat[i,j+1] != mat[i,j]:
			res += 2
		return res

	def point_index( i, j, label):
		res = 0
		if i != 0 and mat[i-1,j] == mat[i,j] and set_mat[i-1,j] == 0:
			set_mat[i-1,j] = label
			point_index(i-1,j,label)
		if i != mat.shape[0]-1 and mat[i+1,j] == mat[i,j] and set_mat[i+1,j] == 0:
			set_mat[i+1,j] = label
			point_index(i+1,j,label)
		if j != 0 and mat[i,j-1] == mat[i,j] and set_mat[i,j-1] == 0:
			set_mat[i,j-1] = label
			point_index(i,j-1,label)
		if j != mat.shape[1]-1 and mat[i,j+1] == mat[i,j] and set_mat[i,j+1] == 0:
			set_mat[i,j+1] = label
			point_index(i,j+1,label)

	for i in range(edges.shape[0]):
		for j in range(edges.shape[1]):
			edges[i,j] = check(mat,i,j)

	set_number = 1
	for i in range(edges.shape[0]):
		for j in range(edges.shape[1]):
			if set_mat[i,j] == 0:
				set_mat[i,j] = set_number
				point_index(i,j,set_number)
				set_number += 1
	return edges, set_mat

def label_index(index_mat, edges, X, Y, fig, ax, set_mat=None):
	"""
	Helper function to label index in figure.
	"""
	X = np.sort(X)
	Y = np.sort(Y)
	def extract_bit(num, index):
		mask = 1 << index
		return int(num & mask > 0)

	for i in range(edges.shape[0]):
		for j in range(edges.shape[1]):
			num = edges[i,j]
			bit0 = extract_bit(num,0)
			bit1 = extract_bit(num,1)
			bit2 = extract_bit(num,2)
			bit3 = extract_bit(num,3)
			if bit0:
				ax.hlines(Y[i]+0.5,X[j]-0.5, X[j+1]-0.5)
			if bit1:
				ax.vlines(X[j+1]-0.5,Y[i]-0.5, Y[i+1]-0.5)
			if bit2:
				ax.hlines(Y[i]-0.5,X[j]-0.5, X[j+1]-0.5)
			if bit3:
				ax.vlines(X[j]-0.5,Y[i]-0.5, Y[i+1]-0.5)
	if set_mat is not None:
		set_flag_array = []
		y_per_dot = 1.0 / set_mat.shape[0]
		x_per_dot = 1.0 / set_mat.shape[1]
		for i in range(set_mat.shape[0]):
			for j in range(set_mat.shape[1]):
				if set_mat[i,j] not in set_flag_array:
					set_flag_array += [set_mat[i,j]]
					if index_mat[i,j] != 'nan':
						ax.text(j,set_mat.shape[0]-i-1 , index_mat[i,j], horizontalalignment='center', verticalalignment='center', fontsize=12,fontweight="ultralight", transform=ax.transData)
					
def mapping_label_to_ticks(target_max, axle_max, target_label):
	return [(x*axle_max / target_max)-.5 for x in target_label]

def generate_pgm(data, dataset, error_bound=64, log=False):
	row = data[(data['dataset'] == dataset) & \
							(data['error_bound'] == error_bound)]
	if not row.empty:
		pgm = row.iloc[0]["pgm"]
		size = row.iloc[0]["table_size"]
		if not log:
			return pgm
		else:
			return math.log(pgm)
	else:
		print("pgm for", dataset, "is not available!")

def generate_outlier(data, dataset):
	df = data[data['dataset'] == dataset]
	if not df.empty:
		return df.iloc[0]['outlier']
	else:
		print("outlier for", dataset, "is not available!")

def get_index_style(name:str):
	LINE_MARKS = ["o", "o", "v", "v", "+", "P", "P", "x", "x", "2", "2", "3", "4","*"]
	LINE_STYLES = ['--', '-.']
	LINE_FULLSTYLE = ["none", "full"]
	LINE_COLOR = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:brown', 'tab:purple', 'tab:pink', 'tab:gray', 'tab:olive', 'tab:cyan','black']

	style_map = {
		'A+': ['v', "tab:blue"], 
		'A': ['v', "tab:blue"], 
		'L+': ['o', "tab:orange"], 
		'L++': ['o', "tab:orange"], 
		'L+++': ['o', "tab:orange"], 
		'L': ['o', "tab:orange"], 
		'X': ['d', "tab:gray"],
		'P': ['d', "tab:pink"],
		'r+': ['P', "olive"], 
		'r': ['P', "olive"], 
		'b': ['2', "black"], 
		'b+': ['2', "black"], 
		'h+': ['x', "tab:red"], 
		'h': ['x', "tab:red"], 
		'm': ['|', 'tab:green'],
		'w': ['3', 'tab:purple'],
	}

	idx_index = ["L", "L+", "L++", "L+++", "A", "A+", "X", "r", "r+", "b", "b+", "h", "h+", "m", "P", "F", "w"]
	i = idx_index.index(name)
	ret = {
		"marker": LINE_MARKS[i % len(LINE_MARKS)],
		"fillstyle": "full",
		"linestyle": LINE_STYLES[i % len(LINE_STYLES)],
		"color": LINE_COLOR[i % len(LINE_COLOR)],
		# "markersize":6,
		# "markeredgewidth":2,
		"linewidth": 1,
		"hatch": hatches[i % len(hatches)]
	}
	if i is None:
		raise Exception(f"Unknow index {name} for get_index_style() ")

	if name in style_map:
		ret['marker'] = style_map[name][0]
		ret['color'] = style_map[name][1]
	
	if name[0].isupper(): 
		ret["linestyle"] = 'solid'
	else:
		ret["linestyle"] = 'dotted'

	return ret 

def plot_2d_heatmap_pgm(data, learned_index, traditional_index):
	fig = plt.figure(figsize=(8, 5),constrained_layout=True)
	spec = gridspec.GridSpec(ncols=1, nrows=1, figure=fig)
	axspec = spec[0,0]
	ax = fig.add_subplot(axspec)

	data["order_val"] = data["pgm"]
	data.sort_values(by=["order_val", "insert_ratio"], inplace=True)
	# extract heatmap data
	(read_ratio, pgm,heatmap_learned, index_learned) = extract_heatmap_data(data, learned_index, use_abbrev=True)
	(read_ratio, pgm,heatmap_traditional, index_traditional) = extract_heatmap_data(data, traditional_index, use_abbrev=True)

	# Calculate ratio value
	heatmap_single = heatmap_traditional - heatmap_learned
	single_min = (heatmap_traditional >= heatmap_learned) * heatmap_traditional + (heatmap_traditional < heatmap_learned) * heatmap_learned
	heatmap_single /= single_min

	# construct index mat
	index_mat = np.array(index_learned)
	for i in range(heatmap_single.shape[0]):
		for j in range(heatmap_single.shape[1]):
			if heatmap_single[i,j]>0:
				index_mat[i,j] = index_traditional[i,j]
	
	X = pgm
	X = X.astype(int)
	Y =  (1- read_ratio)
	maximum = 0.75
	im = ax.imshow(np.flipud(heatmap_single), vmin=-maximum, vmax=maximum, cmap="coolwarm")
	edges, set_mat = get_index_area(index_mat)
	label_index(index_mat, np.flipud(edges), np.arange(heatmap_single.shape[1]+1), np.arange(heatmap_single.shape[0]+1), fig, ax,set_mat)

	target_ylabel = np.arange(0, 1.01, 0.2)
	ax.set_xticks(np.arange(data["pgm"].unique().shape[0]))
	ax.set_xticklabels(["{:.2e}".format(x) for x in data["order_val"].unique()], rotation=75) # .astype(int))
	ax.set_yticks(mapping_label_to_ticks(Y[-1], heatmap_single.shape[0], target_ylabel))
	ax.set_yticklabels([f"{y:.2f}" for y in target_ylabel[::-1]])

	ax.set_xlabel("data hardness (thousand)")
	ax.set_ylabel("write ratio")

	sorted_dataset = data["dataset"].unique()
	sorted_dataset = [(data[:25] + '..') if len(data) > 25 else data for data in sorted_dataset]
	for i, xpos in enumerate(ax.get_xticks()):
		ax.text(xpos+0.3, -0.8, sorted_dataset[i], size = 9, ha = 'center', rotation=75)

	fig.colorbar(im,ax=ax)
	return fig

def plot_3d_heatmap(data, learned_index, traditional_index, plot_on='insert', tied_perc=None, maximum_ratio=None, view_angle=(53,-35)):
	# creating figures
	np.random.seed(2071)
	fig = plt.figure(figsize=(8, 8))
	ax = fig.add_subplot(111, projection='3d')

	ax.xaxis.pane.fill = False
	ax.yaxis.pane.fill = False
	ax.zaxis.pane.fill = False
	ax.xaxis.pane.set_edgecolor('w')
	ax.yaxis.pane.set_edgecolor('w')
	ax.zaxis.pane.set_edgecolor('w')

	if plot_on == 'insert':	
		data.sort_values(by=["insert_ratio", "pgm"], inplace=True)
	elif plot_on == 'delete':
		data.sort_values(by=["delete_ratio", "pgm"], inplace=True)
	ds_name = data["dataset"].unique().tolist()*5

	# extract heatmap data
	(read_ratio, pgm,heatmap_learned, index_learned) = extract_heatmap_data(data, learned_index, use_abbrev=True)
	(read_ratio, pgm,heatmap_traditional, index_traditional) = extract_heatmap_data(data, traditional_index, use_abbrev=True)

	# Calculate ratio value
	heatmap_single = heatmap_traditional - heatmap_learned
	single_min = (heatmap_traditional >= heatmap_learned) * heatmap_traditional + (heatmap_traditional < heatmap_learned) * heatmap_learned
	heatmap_single /= single_min

	X = pgm
	X = X.astype(int)
	Y =  (1- read_ratio)

	# construct index mat
	index_mat = np.array(index_learned)
	for i in range(heatmap_single.shape[0]):
		for j in range(heatmap_single.shape[1]):
			if heatmap_single[i,j]>0:
				index_mat[i,j] = index_traditional[i,j]

	# tied
	if tied_perc is not None:
		for i in range(heatmap_single.shape[0]):
			for j in range(heatmap_single.shape[1]):
				if abs(heatmap_single[i, j]) < tied_perc:
					heatmap_single[i, j] = 0
					index_mat[i, j] = "T"

	x = np.array((np.array(data["pgm"].unique().tolist())).tolist() * 5)

	y = data[['dataset', 'pgm_global']].drop_duplicates()['pgm_global'].to_list() * 5
	z = []

	for write in Y:
		for _ in range(heatmap_single.shape[1]):
			z.append(round(write, 1))

	# creating the heatmap

	exist_label = []
	exist_ds = []
	single_abbr2index = {v: k for k, v in index_abbreviation.items()}

	# Set view angle
	ax.view_init(elev=view_angle[0], azim=view_angle[1]) # i used -42 for revision letter

	if maximum_ratio:
		maximum = maximum_ratio
	else:
		maximum = np.abs(heatmap_single.flatten()).max()
	for x_i, y_i, z_i, c_i,idx_i,ds_i in zip(x,y,z,heatmap_single.reshape(-1),index_mat.reshape(-1), ds_name):
		img = ax.scatter(x_i, y_i, z_i, c = [c_i], vmin=-maximum, vmax=maximum, marker=get_index_style(idx_i)["marker"], edgecolors='black', linewidth=1,
				   s=100, cmap="RdBu_r", linewidths=3.5, label=index_display[single_abbr2index[idx_i]] if not idx_i in exist_label else '_nolabel_')
		exist_label.append(idx_i)
		if ds_i not in exist_ds:
			z = 1.2
			offset = 0

			dataset_display_name = dataset_abbreviation[ds_i] 
			# Shift the labels manual for display
			if dataset_display_name == 'history':
				z = 1.5
			if dataset_display_name == 'books':
				z = 1.4
			if dataset_display_name == 'covid':
				z = 1.4
			if dataset_display_name == 'eth_gas':
				offset = -70000
				dataset_display_name = "eth"
			if dataset_display_name == 'planetways':
				z = 1.4
				offset = 50000
			if dataset_display_name == 'gnomad':
				z = 1.7
				offset = -50000
			if dataset_display_name == 'wise' and view_angle[1]<-35: # only for the 3D in letters
				offset = 50000
			if dataset_display_name == 'wiki_rev':
				dataset_display_name = "wiki"
				z=1.6
			



			# fixing for the stupid data label of matplotlib
			if dataset_display_name == "planet" and view_angle[1] < -35:
				# x2, y2, _ = proj3d.proj_transform(x_i, y_i, z, ax.get_proj())
				ax_text = ax.text2D(0.68, 0.85, dataset_display_name, fontsize=12, rotation=270, transform=ax.transAxes)
				# ax_text.set_position((x2, y2))
			elif dataset_display_name == "fb" and view_angle[1] < -35:
				ax_text = ax.text2D(0.71, 0.65, dataset_display_name, fontsize=12, rotation=270, transform=ax.transAxes)
			elif dataset_display_name == "genome" and view_angle[1] < -35:
				ax_text = ax.text2D(0.735, 0.55, dataset_display_name, fontsize=12, rotation=270, transform=ax.transAxes)
			elif dataset_display_name == "genome" and view_angle[1] == -35:
				ax_text = ax.text2D(0.55, 0.43, dataset_display_name, fontsize=12, rotation=270, transform=ax.transAxes)
			elif dataset_display_name == "osm" and view_angle[1] == -35 :
				ax_text = ax.text2D(0.73, 0.83, dataset_display_name, fontsize=12, rotation=270, transform=ax.transAxes)
			elif dataset_display_name == "syn_ghard_leasy":
				ax_text = ax.text2D(0.59, 0.8, dataset_display_name, fontsize=12, rotation=270, transform=ax.transAxes)
			elif dataset_display_name == "syn_genome":
				ax_text = ax.text2D(0.525, 0.5, dataset_display_name, fontsize=12, rotation=270, transform=ax.transAxes)
			elif dataset_display_name == "syn_osm":
				ax_text = ax.text2D(0.67, 0.8, dataset_display_name, fontsize=12, rotation=270, transform=ax.transAxes)
			elif dataset_display_name == "syn_ghard_lhard":
				ax_text = ax.text2D(0.87, 0.65, dataset_display_name, fontsize=12, rotation=270, transform=ax.transAxes)

			else:
				ax_text_org = ax.text(x_i + offset, y_i, z , dataset_display_name, zdir=(0,0,1), fontsize=12)

				# ax_text.set_position((x2, y2))				
		exist_ds.append(ds_i)
	
	ax.set_zticks(Y)
	xtick = ax.get_xticks()
	ax.set_xticks(xtick)
	# fig.colorbar(img, shrink=0.5)

	# adding title and labels
	# ax.set_title("State-of-the-art Throughput Combat")
	ax.set_xlabel('Local hardness $H_{PLA (\epsilon=' + str(g_e_local) + ')}$')
	ax.set_ylabel('Global hardness $H_{PLA (\epsilon=' + str(g_e_global) + ')}$')
	ax.set_xlim(left=0) # HERE
	if plot_on == 'delete':
		ax.set_zlabel('Delete Ratio')
	else:
		ax.set_zlabel('Write Ratio')
	ax.legend()
				
	ax.xaxis.set_major_formatter(OOMFormatter(3))
	ax.yaxis.set_major_formatter(OOMFormatter(2))
	
	# Z axis data to %
	vals = ax.get_zticks()
	ax.set_zticklabels(['{:,.0%}'.format(x) for x in vals])

	# Add easy, diffcult
	ax.text(-9000 ,-1300, 0.2 ,"Easy", fontsize=14, weight="bold")
	ax.text(ax.get_xlim()[1] + 9000 ,ax.get_ylim()[1] + 100, -0.3 ,"Difficult", fontsize=14, weight="bold")

	return fig

def plot_2d_heatmap(data, learned_index, traditional_index, tied_perc=None, maximum_ratio=None):
	fig, ax = plt.subplots(figsize=(8,3),constrained_layout=True)

	data.sort_values(by=["insert_ratio", "pgm"], inplace=True)
	ds_name = data["dataset"].unique().tolist()*5

	# extract heatmap data
	(read_ratio, pgm,heatmap_learned, index_learned) = extract_heatmap_data(data, learned_index, use_abbrev=True)
	(read_ratio, pgm,heatmap_traditional, index_traditional) = extract_heatmap_data(data, traditional_index, use_abbrev=True)

	# Calculate ratio value
	heatmap_single = heatmap_traditional - heatmap_learned
	single_min = (heatmap_traditional >= heatmap_learned) * heatmap_traditional + (heatmap_traditional < heatmap_learned) * heatmap_learned
	heatmap_single /= single_min

	X = pgm
	# X = X.astype(int)
	Y =  (1- read_ratio)

	# construct index mat
	index_mat = np.array(index_learned)
	for i in range(heatmap_single.shape[0]):
		for j in range(heatmap_single.shape[1]):
			if heatmap_single[i,j]>0:
				index_mat[i,j] = index_traditional[i,j]

	# tied
	if tied_perc is not None:
		for i in range(heatmap_single.shape[0]):
			for j in range(heatmap_single.shape[1]):
				if abs(heatmap_single[i, j]) < tied_perc:
					heatmap_single[i, j] = 0
					index_mat[i, j] = "T"

	x = np.array((np.array(data["pgm"].unique().tolist())).tolist() * 5)
	y = []

	for write in Y:
		for _ in range(heatmap_single.shape[1]):
			y.append(round(write, 1))

	# creating the heatmap

	exist_label = []
	single_abbr2index = {v: k for k, v in index_abbreviation.items()}

	if maximum_ratio:
		maximum = maximum_ratio
	else:
		maximum = np.abs(heatmap_single.flatten()).max()
	for x_i, y_i, c_i,idx_i,ds_i in zip(x,y,heatmap_single.reshape(-1),index_mat.reshape(-1), ds_name):
		img = ax.scatter(x_i, y_i, c = [c_i], vmin=-maximum, vmax=maximum, marker=get_index_style(idx_i)["marker"], s=80, cmap="RdBu_r", linewidths=3.5, label=index_display[single_abbr2index[idx_i]] if not idx_i in exist_label else '_nolabel_')
		exist_label.append(idx_i)
		if (y_i == 1):
			ax.text(x_i, y_i+0.1, dataset_abbreviation[ds_i], fontsize='x-small', rotation='90')

	ax.set_yticks(Y)
	xtick = ax.get_xticks()
	ax.set_xticks(xtick)
	fig.colorbar(img, shrink=0.5)

	# adding title and labels
	ax.set_title("State-of-the-art Throughput Combat")
	ax.set_xlabel('PGM')
	ax.set_ylabel('Write Ratio')
	ax.legend()
	return fig

def plot_scalability(data, datasets, indexes = None, threads = [2,4,8,16,24], workloads = [0,0.5,1], numa = False, ht = False):
	workload_label = {0: 'RO', 0.2: 'RH', 0.5: 'BAL', 0.8: 'WH', 1: 'WO'}
	datasets = [dataset_abbreviation_rev[d] for d in datasets]
	if indexes is None:
		indexes = data["index_type"].unique()
	fig, axs = plt.subplots(len(workloads), len(datasets), figsize=(2.75*len(datasets), 2*len(workloads)), sharex='col', constrained_layout=True) #, sharey='row'  figsize=(1.75*len(datasets), 1.25*len(workloads)
	for idx, dataset in enumerate(datasets):
		for iidx, workload in enumerate(workloads):
			max_val = 0
			ax = axs[iidx, idx]
			if iidx == 0:
				ax.set_title(dataset_abbreviation[dataset])
			for iiidx, index in enumerate(indexes):
				value = data[(data["dataset"] == dataset) & (data["insert_ratio"] == workload) & (data["index_type"] == index) &(data.thread_num.isin(threads))].groupby("thread_num").agg('mean').sort_values(by="thread_num")["throughput"]
				value = value / 1e6

				styles = get_index_style(index_abbreviation[index])
				del styles["hatch"]
				styles["markersize"] = 10
				if index_abbreviation[index] == "r+":
					styles["marker"] = "+"

				ax.plot(threads, value, label=index_display[index] if idx == 0 and iidx == 0 else None, **styles) #if idx == 0 and iidx == 0 else None
				if value.max() > max_val:
					max_val = value.max()
			if idx == 0:
				ax.set_ylabel(f"{workload_label[workload]} Throughput (Mop/s)", fontsize='small')
			if not numa:
				ax.set_xticks(threads)
				ax.set_xticklabels(threads)
			else:
				num_core_per_numa = 24
				ax.set_xticks([2] + [x for x in threads if x >= 24])
				ax.set_xticklabels(["2 cores"] + [int(x / num_core_per_numa) for x in threads if x >= 24])
				ax.vlines([x for x in threads if x >= 24], ymin=0, ymax=max_val, ls='--', lw=1, color='tab:grey')
			# ax.legend(fontsize='xx-small')

			# add shadow for numa 
			ax.set_xlim(2, max(threads))
			if numa:
				ax.axvspan(36, 96, color='grey', alpha=0.2, lw=0)
			elif ht:
				ax.axvspan(30, 48, color='grey', alpha=0.2, lw=0)
			
			if iidx == len(workloads) - 1:
				if numa:
					ax.set_xlabel("# of sockets")
				else:
					ax.set_xlabel("# of cores")

	# fig.subplots_adjust(bottom=0.1)
	fig.legend(bbox_to_anchor=(1.15,0.7))
	# fig.legend(bbox_to_anchor=(1,0), bbox_transform=fig.transFigure, ncol=3)
	# fig.tight_layout()

	# Set x-axle
	# fig.legend(bbox_to_anchor=(1,0.5))
	return fig

def plot_memory(data, datasets, indexes):
	datasets = [dataset_abbreviation_rev[d] for d in datasets]
	fig, axs = plt.subplots(1, len(datasets), figsize=(6, 1.5), sharey=True, constrained_layout=True)
	if not indexes:
		indexes = data['index_type'].unique()
	for idx, dataset in enumerate(datasets):
		ax = axs[idx]
		ax.set_title(dataset_abbreviation[dataset])
		for iidx, index in enumerate(indexes):
			value = data[(data['index_type'] == index) & (data['dataset'] == dataset)]['memory_consumption'].agg('mean')
			value /= math.pow(2,30)
			ax.bar(iidx, value, edgecolor='black', alpha=0.9, label=index_display[index] if idx == 0 else None, hatch=hatches[iidx], color=get_index_style(index_abbreviation[index])['color'])
			if idx == 0:
				ax.set_ylabel("Size (GB)")
		# for index in shade:
		# 	value = data_index[(data_index['index_type'] == index) & (data_index['dataset'] == dataset)]['memory_consumption']
		# 	value /= math.pow(2,30)
		# 	print(index, value)
		# 	ax.bar(indexes.index(index), value, edgecolor='black', facecolor="none", hatch="////", label="index" if iidx == 0 else None)
		# if idx == 0:
		# ax.legend()
		ax.set_xticklabels([])
		ax.set_xticks([])
		# ax.set_xticks([idx for idx in range(len(indexes))])
		# labels = [index_display[index] for index in indexes]
		# ax.set_xticklabels([index if index != "PGM-Index" else "PGM" for index in labels], fontsize='small')

		# Add seperation line between traditional and learned index
		ax.axvline(len(indexes) / 2 - .5, color='black', linestyle='--')

	fig.legend(bbox_to_anchor=(1,0), bbox_transform=fig.transFigure, ncol=int(len(indexes)))
	return fig

def plot_latency(data, datasets, thread_num, indexes=None, indexes_std=None, file_name_prefix=""):
	datasets = [dataset_abbreviation_rev[d] for d in datasets]
	cats = ["99.9 percentile", "latency_variance"]
	cats_label = {"99.9 percentile": "99.9% (ns)",
	 "latency_variance": "STD (ns)"}
	if not indexes:
		indexes = data['index_type'].unique()
	for wr in [0, 1]:
		# subfig = subfigs[wr]
		fig = plt.figure(figsize=(5,2.5), constrained_layout=True)
		# fig.suptitle('Tail insert latency' if wr == 1 else 'Tail lookup latency')
		fig.text(0, 1, '(a) Single core' if thread_num == 1 else '(b) 24 cores', fontweight='bold')
		axs = fig.subplots(len(cats), len(datasets), sharey='row')
		for idx, cat in enumerate(cats):
			# if cat == "latency_variance":
			# 	temp_index = [i for i in indexes if i != 'xindex']
			# else:
			# 	temp_index = indexes
			if thread_num == 1 and wr == 0 and idx == 0:
				ylim_max = 4000
			elif thread_num == 1 and wr == 0 and idx == 1:
				ylim_max = 10000
			elif thread_num == 1 and wr == 1 and idx == 0:
				ylim_max = 6000
			elif thread_num == 1 and wr == 1 and idx == 1:
				ylim_max = 10000
			elif thread_num > 1 and wr ==0 and idx == 0:
				ylim_max = 4000
			elif thread_num > 1 and wr ==0 and idx == 1:
				ylim_max = 3000
			elif thread_num > 1 and wr==1 and idx == 0:
				ylim_max = 40000
			elif thread_num > 1 and wr==1 and idx == 1:
				ylim_max = 50000
			else:
				raise Exception(f"thread_num: {thread_num}, wr: {wr}, idx: {idx}")

			has_overflow = False
			for iidx, dataset in enumerate(datasets):
				ax = axs[idx, iidx]
				if iidx == 0:
					ax.set_ylabel(cats_label[cat])
				for iiidx, index in enumerate(indexes):
					value = data[(data["dataset"] == dataset) & (data["insert_ratio"] == wr) & (data["index_type"] == index) & (data["thread_num"] == thread_num)][cat].agg('mean')
					if cat == "latency_variance":
						value = math.sqrt(value)

					# min bar
					if value < ylim_max*.05:
						value = ylim_max*.05 
					ax.bar(iiidx, value, label=index_display[index] if idx+iidx==0 else None, edgecolor='black', color=get_index_style(index_abbreviation[index])['color'], hatch=get_index_style(index_abbreviation[index])['hatch'], alpha=0.9)

					# add text on top of the bar if overflow 
					if value > ylim_max:
						# ax.text(iiidx - .5, ylim_max + ylim_max*.05, f"{int(math.ceil(value/ylim_max))}x", fontsize='small', rotation=90)
						ax.text(iiidx - .5, ylim_max + ylim_max*.05, f"{(value/ylim_max):.1f}x", fontsize='small', rotation=90)
						has_overflow = True
					
				# Add seperation line between traditional and learned index
				if thread_num == 1:
					ax.axvline(indexes.index("artunsync") - .5, color='black', linestyle='--')
				else:
					ax.axvline(indexes.index("artolc") - .5, color='black', linestyle='--')

				# if idx == len(cats)-1 and iidx == len(datasets)-1:
					# ax.legend(fontsize='x-small')
				ax.set_xticklabels([])
				ax.set_xticks([])
				
				# Hide Xindex in std
				ax.set_ylim(0, ylim_max)

			# Fill the title of dataset
			y_dataset_title = 1.0
			if has_overflow: 
				y_dataset_title = 1.4
			if idx == 0:
				for iidx, dataset in enumerate(datasets):
					ax = axs[idx, iidx]
					ax.set_title(dataset_abbreviation[dataset], y=y_dataset_title)

		# if wr == 1:			
			# Show legend
		fig.legend(bbox_to_anchor=(1,0), bbox_transform=fig.transFigure, ncol=4)
		fig.savefig(f'./{file_name_prefix}_{wr}.pdf', bbox_inches="tight")

def plot_range_query(data, thread_num=1, indexes=None, datasets = ['covid', 'libio', 'osm', 'genome']):
	if indexes == None:
		indexes = data['index_type'].unique()
	datasets = [dataset_abbreviation_rev[d] for d in datasets]
	# data.drop(data[data['scan_num'] == 50].index, inplace=True)
	data['throughput'] = data['throughput'] * data['scan_num']
	fig, axs = plt.subplots(1, 4, constrained_layout=True, figsize=(1.75*len(datasets), 2))
	for idx, dataset in enumerate(datasets):
		ax = axs[idx]
		ax.set_title(dataset_abbreviation[dataset])
		for index in indexes:
			temp = data[(data['dataset'] == dataset) & (data['index_type'] == index) & (data['thread_num'] == thread_num)].groupby("scan_num").agg('mean').sort_values(by="scan_num")
			
			# range(temp['scan_num'].shape[0])
			ax.plot(range(len(data['scan_num'].unique())), temp['throughput'] / 1e6, label=index_display[index] if idx == 0 else None, marker=get_index_style(index_abbreviation[index])['marker'], linestyle=get_index_style(index_abbreviation[index])['linestyle'], color=get_index_style(index_abbreviation[index])['color'])
		ax.set_xticks(range(len(data['scan_num'].unique())))
		ax.set_xticklabels(range(1, len(data['scan_num'].unique())+1))
		ax.set_xlabel("Range $\mathregular{10^x}$")
	axs[0].set_ylabel("Throughput (M keys/s)")
	fig.legend(bbox_to_anchor=(.8,0), bbox_transform=fig.transFigure, ncol=3)
	return fig

def plot_datashift(data, data_original, thread_num, indexes=None, datasets = ['covid->osm', 'osm->covid', 'covid->genome', 'genome->covid']):
	if indexes == None:
		indexes = data['index_type'].unique()
	datasets = [dataset_abbreviation_rev[d] for d in datasets]
	fig, axs = plt.subplots(1, len(datasets), constrained_layout=True, figsize=(1.5*len(datasets), 1.75), sharey='row')
	for idx, dataset in enumerate(datasets):
		ax = axs[idx]
		ax.set_title(dataset_abbreviation[dataset])
		dataset_original = dataset_abbreviation[dataset].split('->')[0]
		for iidx, index in enumerate(indexes):
			value = ((data[(data['dataset'] == dataset) & \
				(data['index_type'] == index) & \
				(data['thread_num'] == thread_num)]["throughput"].agg('mean') / \
				data_original[(data_original['dataset'] == dataset_abbreviation_rev[dataset_original]) & \
				(data_original['index_type'] == index) & \
				(data_original['read_ratio'] == 0.5) & \
				(data_original['thread_num'] == thread_num)]["throughput"].agg('mean')) - 1) * 100
			ax.bar(iidx, value, label=index_display[index]if idx == 0 else None, edgecolor='black', color=get_index_style(index_abbreviation[index])['color'], hatch=get_index_style(index_abbreviation[index])['hatch'], alpha=0.9)
		ax.set_xticks([])
		ax.set_xticklabels([])
		if idx == 0:
			ax.set_ylabel("Throughput % change")
		fig.legend(bbox_to_anchor=(1,0), bbox_transform=fig.transFigure, ncol=int((len(indexes)+1)/2))

		# Add seperation line between traditional and learned index
		if thread_num == 1:
			ax.axvline(indexes.index("artunsync") - .5, color='black', linestyle='--')
		else:
			ax.axvline(indexes.index("artolc") - .5, color='black', linestyle='--')
	return fig

if __name__ == "__main__":
	plt.rcParams['pdf.fonttype'] = 42

	e_local = 32
	e_global = 4096
	g_e_local = e_local
	g_e_global = e_global
	
	pgm_file = read_data('./pgm_val.csv')

	########## Single-Threading ##########
	data = read_data('./data/st_new.csv')
	data["pgm"] = data["dataset"].apply(lambda s: generate_pgm(pgm_file, s, e_local))
	data["pgm_global"] = data["dataset"].apply(lambda s: generate_pgm(pgm_file, s, e_global))

	for to_drop in ['eth', 'wiki_rev', 'planetways', 'gnomad']:
		data.drop(data[data['dataset'].str.contains(to_drop)].index, inplace=True)
	fig = plot_3d_heatmap(
		data,
		["xindex", "alex", "lipp", "finedex"],
		['btree', 'hot', 'artunsync', 'wormhole_u64', 'masstree'],
		maximum_ratio=0.4
	)
	fig.savefig('./3d_st.pdf', bbox_inches='tight')

	########## Multi-Threading ##########
	data = read_data('./data/mt_new.csv')
	for to_drop in ['eth', 'wiki_rev', 'planetways', 'gnomad']:
		data.drop(data[data['dataset'].str.contains(to_drop)].index, inplace=True)
	data["pgm"] = data["dataset"].apply(lambda s: generate_pgm(pgm_file, s, e_local))
	data["pgm_global"] = data["dataset"].apply(lambda s: generate_pgm(pgm_file, s, e_global))

	fig = plot_3d_heatmap(
		data[data['thread_num'] == 24],
		["xindex", "alexol", "finedex", "lippol"],
		['btreeolc', 'hotrowex', 'artolc', 'wormhole_u64', 'masstree']
	)
	fig.savefig('./3d_mt.pdf', bbox_inches='tight')


	fig = plot_scalability(
		pd.concat([data, read_data('./data/ht_new.csv')]),
		['covid', 'libio', 'genome', 'osm'],
		['alexol', 'lippol', 'xindex', 'finedex', 'artolc', 'btreeolc', 'hotrowex', 'masstree', 'wormhole_u64'],
		[2, 4, 8, 16, 24, 36, 48], ht = True
	)
	fig.savefig('./scalability_ht.pdf', bbox_inches="tight")

	fig = plot_scalability(
		pd.concat([data, read_data('./data/numa_new.csv')]),
		['covid', 'libio', 'genome', 'osm'],
		['alexol', 'lippol', 'xindex', 'finedex', 'artolc', 'btreeolc', 'hotrowex', 'masstree', 'wormhole_u64'],
		[2, 24, 48, 72, 96], numa=True
	)
	fig.savefig('./scalability_numa.pdf', bbox_inches="tight")

	######### Memory ##########
	data = read_data('./data/mem_new.csv')
	fig = plot_memory(data, ['covid', 'libio', 'genome', 'osm'], ['alex','lipp', 'pgm','artunsync','btree','hot'])
	fig.savefig('./memory.pdf', bbox_inches="tight")

	########## Robustness ##########
	data = read_data('./data/latency_new.csv')
	fig = plot_latency(
		data,
		['covid', 'libio', 'genome', 'osm'],
		1,
		['alex', 'lipp', 'pgm', 'xindex', 'finedex', 'artunsync', 'hot', 'btree', 'wormhole_u64', 'masstree'],
		file_name_prefix="latency"
	)

	fig = plot_latency(
		data,
		['covid', 'libio', 'genome', 'osm'],
		24,
		['alexol', 'lippol', 'xindex', 'finedex', 'artolc', 'hotrowex', 'btreeolc', 'wormhole_u64', 'masstree'],
		file_name_prefix="latency_mt"
	)

	fig = plot_datashift(
		read_data('./data/datashift_new.csv'),
		read_data('./data/st_new.csv'),
		1,
		['alex', 'lipp', 'pgm', 'xindex', 'finedex', 'artunsync', 'hot', 'btree', 'wormhole_u64', 'masstree']
	)
	fig.savefig('./datashift.pdf', bbox_inches="tight")

	fig = plot_range_query(read_data('./data/range.csv'), 1, ['alex', 'lipp', 'finedex', 'pgm', 'btree', 'hot'])
	fig.savefig('./range.pdf', bbox_inches="tight")

	# plt.show()