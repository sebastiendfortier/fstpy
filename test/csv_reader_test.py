# -*- coding: utf-8 -*-
import pytest
import numpy as np
import fstpy.all as fstpy
from test import TEST_PATH
pytestmark = [pytest.mark.unit_tests]

@pytest.fixture
def plugin_test_dir():
   return TEST_PATH + '/ReaderCsv/testsFiles/'


def test_1(plugin_test_dir):
	"""Test with pds1_level2.new.csv. It should return a DataFrame"""
	src = plugin_test_dir + 'pds1_level2.new.csv'
	df = fstpy.CsvFileReader(path=src,encode_ip1=False).to_pandas()

	NI = 3
	NJ = 2
	NK = 1

	d={'nomvar':(1,"CSV"),'etiket':(1,'CSVREADER'),'nbits':(1,24),'datyp':(1,1),'grtyp':(1,'X'),'typvar':(1,'X'),
	   'ip2':(1,0),'ip3':(1,0),'ig1':(1,0),'ig2':(1,0),'ig3':(1,0),'ig4':(1,0),'npas':(1,0),'grid':(1,'00'),'ni':(1,NI),
	   'nj':(1,NJ),'nk':(1,NK)}

	for k,v in d.items():
		assert(df[k].unique().size == v[0])
		assert(df[k].unique()[0] == v[1])
		
	assert(df.ip1.unique().size == 2)
	assert(df.ip1.unique()[0] == 1)
	assert(df.ip1.unique()[1] == 0)

	d1 = np.array([[11.1, 22.2], [33.3, 44.4], [55.5, 66.6]],dtype=np.float32) 
	d2 = df.d[0]
	d3 = np.array([[77.7, 88.8], [99.9, 100.1], [110.11, 120.12]],dtype=np.float32)
	d4 = df.d[1]

	assert(np.array_equal(d1,d2))
	assert(np.array_equal(d3,d4))
	assert(df.d.size == 2)
	
	
	

def test_2(plugin_test_dir):
	"""Test with gds1_pds1_level2.new.csv. It should return a DataFrame"""
	src = plugin_test_dir + 'gds1_pds1_level2.new.csv'
	df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()

	NI = 3
	NJ = 2
	NK = 1

	d={'nomvar':(1,"CSV"),'etiket':(1,'CSVREADER'),'nbits':(1,24),'datyp':(1,1),'grtyp':(1,'X'),'typvar':(1,'X'),
	   'ip2':(1,0),'ip3':(1,0),'ig1':(1,0),'ig2':(1,0),'ig3':(1,0),'ig4':(1,0),'npas':(1,0),'grid':(1,'00'),'ni':(1,NI),
	   'nj':(1,NJ),'nk':(1,NK)}

	for k,v in d.items():
		assert(df[k].unique().size == v[0])
		assert(df[k].unique()[0] == v[1])

	assert(df.ip1.unique().size == 2)
	assert(df.ip1.unique()[0] == 1)
	assert(df.ip1.unique()[1] == 0)
	
	d1 = np.array([[11.1, 22.2], [33.3, 44.4], [55.5, 66.6]],dtype=np.float32) 
	d2 = df.d[0]
	d3 = np.array([[77.7, 88.8], [99.9, 100.1], [110.11, 120.12]],dtype=np.float32)
	d4 = df.d[1]

	assert(df.d.size == 2)
	assert(np.array_equal(d1,d2))
	assert(np.array_equal(d3,d4))
	


def test_3(plugin_test_dir):
	"""Test with missing_eol_at_last_line_of_data.new.csv. 
		It should return a DataFrame"""
	src = plugin_test_dir + 'missing_eol_at_last_line_of_data.new.csv'
	df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()

	NI = 3
	NJ = 2
	NK = 1

	d={'nomvar':(1,"CSV"),'etiket':(1,'CSVREADER'),'nbits':(1,24),'datyp':(1,1),'grtyp':(1,'X'),'typvar':(1,'X'),
	   'ip2':(1,0),'ip3':(1,0),'ig1':(1,0),'ig2':(1,0),'ig3':(1,0),'ig4':(1,0),'npas':(1,0),'grid':(1,'00'),'ni':(1,NI),
	   'nj':(1,NJ),'nk':(1,NK)}

	for k,v in d.items():
		assert(df[k].unique().size == v[0])
		assert(df[k].unique()[0] == v[1])

	assert(df.ip1.unique().size == 2)
	assert(df.ip1.unique()[0] == 1)
	assert(df.ip1.unique()[1] == 0)
	
	d1 = np.array([[11.1, 22.2], [33.3, 44.4], [55.5, 66.6]],dtype=np.float32) 
	d2 = df.d[0]
	d3 = np.array([[77.7, 88.8], [99.9, 100.1], [110.11, 120.12]],dtype=np.float32)
	d4 = df.d[1]

	assert(df.d.size == 2)
	assert(np.array_equal(d1,d2))
	assert(np.array_equal(d3,d4))
	


def test_4(plugin_test_dir):
	"""Test with pds1_level2_inversed_level_order.new.csv. 
		It should return a DataFrame"""
	src = plugin_test_dir + 'pds1_level2_inversed_level_order.new.csv'
	df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()

	NI = 3
	NJ = 2
	NK = 1

	d={'nomvar':(1,"CSV"),'etiket':(1,'CSVREADER'),'nbits':(1,24),'datyp':(1,1),'grtyp':(1,'X'),'typvar':(1,'X'),
	   'ip2':(1,0),'ip3':(1,0),'ig1':(1,0),'ig2':(1,0),'ig3':(1,0),'ig4':(1,0),'npas':(1,0),'grid':(1,'00'),'ni':(1,NI),
	   'nj':(1,NJ),'nk':(1,NK)}

	for k,v in d.items():
		assert(df[k].unique().size == v[0])
		assert(df[k].unique()[0] == v[1])
		
	assert(df.ip1.unique().size == 2)
	assert(df.ip1.unique()[0] == 0)
	assert(df.ip1.unique()[1] == 1)
	
	d1 = np.array([[77.7, 88.8], [99.9, 100.1], [110.11, 120.12]],dtype=np.float32) 
	d2 = df.d[0]
	d3 = np.array([[11.1, 22.2], [33.3, 44.4], [55.5, 66.6]],dtype=np.float32)
	d4 = df.d[1]

	assert(df.d.size == 2)
	assert(np.array_equal(d1,d2))
	assert(np.array_equal(d3,d4))
	

def test_5(plugin_test_dir):
	"""Test with with_space.new.csv It should return a DataFrame"""
	src = plugin_test_dir + 'with_space.new.csv'
	df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()

	NI = 3
	NJ = 2
	NK = 1

	d={'nomvar':(1,"CSV"),'etiket':(1,'CSVREADER'),'nbits':(1,24),'datyp':(1,1),'grtyp':(1,'X'),'typvar':(1,'X'),
	   'ip2':(1,0),'ip3':(1,0),'ig1':(1,0),'ig2':(1,0),'ig3':(1,0),'ig4':(1,0),'npas':(1,0),'grid':(1,'00'),'ni':(1,NI),
	   'nj':(1,NJ),'nk':(1,NK)}

	for k,v in d.items():
		assert(df[k].unique().size == v[0])
		assert(df[k].unique()[0] == v[1])
		
	assert(df.ip1.unique().size == 2)
	assert(df.ip1.unique()[0] == 1)
	assert(df.ip1.unique()[1] == 0)
	
	d1 = np.array([[11.1, 22.2], [33.3, 44.4], [55.5, 66.6]],dtype=np.float32) 
	d2 = df.d[0]
	d3 = np.array([[77.7, 88.8], [99.9, 100.1], [110.11, 120.12]],dtype=np.float32)
	d4 = df.d[1]

	assert(df.d.size == 2)
	assert(np.array_equal(d1,d2))
	assert(np.array_equal(d3,d4))
	


def test_6(plugin_test_dir):
	"""Test with with_comments.new.csv It should return a DataFrame"""
	src = plugin_test_dir + 'with_comments.new.csv'
	df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()

	NI = 3
	NJ = 2
	NK = 1

	d={'nomvar':(1,"CSV"),'etiket':(1,'CSVREADER'),'nbits':(1,24),'datyp':(1,1),'grtyp':(1,'X'),'typvar':(1,'X'),
	   'ip2':(1,0),'ip3':(1,0),'ig1':(1,0),'ig2':(1,0),'ig3':(1,0),'ig4':(1,0),'npas':(1,0),'grid':(1,'00'),'ni':(1,NI),
	   'nj':(1,NJ),'nk':(1,NK)}

	for k,v in d.items():
		assert(df[k].unique().size == v[0])
		assert(df[k].unique()[0] == v[1])
		
	assert(df.ip1.unique().size == 2)
	assert(df.ip1.unique()[0] == 1)
	assert(df.ip1.unique()[1] == 0)
	
	d1 = np.array([[11.1, 22.2], [33.3, 44.4], [55.5, 66.6]],dtype=np.float32) 
	d2 = df.d[0]
	d3 = np.array([[77.7, 88.8], [99.9, 100.1], [110.11, 120.12]],dtype=np.float32)
	d4 = df.d[1]

	assert(df.d.size == 2)
	assert(np.array_equal(d1,d2))
	assert(np.array_equal(d3,d4))
	




def test_7(plugin_test_dir):
	"""Test with pds2.new.csv It should return a DataFrame"""
	src = plugin_test_dir + 'pds2.new.csv'
	df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()

	NI = 3
	NJ = 2
	NK = 1

	d={'nomvar':(2,"CSV"),'etiket':(2,'CSVREADER'),'nbits':(1,24),'datyp':(1,1),'grtyp':(1,'X'),'typvar':(1,'X'),
	   'ip2':(1,0),'ip3':(1,0),'ig1':(1,0),'ig2':(1,0),'ig3':(1,0),'ig4':(1,0),'npas':(1,0),'grid':(1,'00'),'ni':(2,NI),
	   'nj':(2,NJ),'nk':(1,NK)}

	for k,v in d.items():
		assert(df[k].unique().size == v[0])
		assert(df[k].unique()[0] == v[1])

	assert(df.nomvar.unique()[1] == "CSV2")
	assert(df.etiket.unique()[1] == "CSVREADER2")
	assert(df.ip1.unique().size == 3)
	assert(df.ip1.unique()[0] == 1)
	assert(df.ip1.unique()[1] == 0)
	assert(df.ip1.unique()[2] == 99)
	assert(df.ni.unique()[1] == 2)
	assert(df.nj.unique()[1] == 3)

	d1 = np.array([[11.1, 22.2], [33.3, 44.4], [55.5, 66.6]],dtype=np.float32) 
	d2 = df.d[0]
	d3 = np.array([[77.7, 88.8], [99.9, 100.1], [110.11, 120.12]],dtype=np.float32)
	d4 = df.d[1]
	d5 = np.array([[1.2, 2.3, 3.4], [4.5, 5.6, 6.7]],dtype=np.float32)
	d6 = df.d[2]

	assert(df.d.size == 3)
	assert(np.array_equal(d1,d2))
	assert(np.array_equal(d3,d4))
	assert(np.array_equal(d5,d6))


def test_8(plugin_test_dir):
	""" Test the csv reader with a file that should not return a dataframe because the dimensions of d are different
		and they have the same etiket and nomvar. Will return the DimensionError"""
	src = plugin_test_dir + 'not_all_same_number_of_lines_in_a_pds.new.csv'
	with pytest.raises(fstpy.DimensionError):
		fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()



def test_9(plugin_test_dir):
	""" Test the csv reader with a file that should not return a dataframe because the dimensions of d are different
		and they have the same etiket and nomvar. Will return the ValueError"""
	src = plugin_test_dir + 'not_all_same_number_of_items_in_lines_of_a_pds.new.csv'
	with pytest.raises(ValueError):
		fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
	

def test_10(plugin_test_dir):
	"""Test to see if I can read a csv file with only a single line per level. 
	   Should return a DataFrame """
	src = plugin_test_dir + 'only_1_line_per_level.new.csv'
	df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()

	NI = 1
	NJ = 6
	NK = 1

	d={'nomvar':(1,"CSV"),'etiket':(1,'CSVREADER'),'nbits':(1,24),'datyp':(1,1),'grtyp':(1,'X'),'typvar':(1,'X'),
	   'ip2':(1,0),'ip3':(1,0),'ig1':(1,0),'ig2':(1,0),'ig3':(1,0),'ig4':(1,0),'npas':(1,0),'grid':(1,'00'),'ni':(1,NI),
	   'nj':(1,NJ),'nk':(1,NK)}

	for k,v in d.items():
		assert(df[k].unique().size == v[0])
		assert(df[k].unique()[0] == v[1])
		
	assert(df.ip1.unique().size == 2)
	assert(df.ip1.unique()[0] == 1)
	assert(df.ip1.unique()[1] == 0)
	
	d1 = np.array([[11.1, 22.2, 33.3, 44.4, 55.5, 66.6]],dtype=np.float32) 
	d2 = df.d[0]
	d3 = np.array([[77.7, 88.8, 99.9, 100.1, 110.11, 120.12]],dtype=np.float32)
	d4 = df.d[1]

	assert(df.d.size == 2)
	assert(np.array_equal(d1,d2))
	assert(np.array_equal(d3,d4))
	

def test_11(plugin_test_dir):
	"""Test to see if I can read a csv file with only a single item per line. 
	   Should return a DataFrame """
	src = plugin_test_dir + 'only_1_item_per_line.new.csv'
	df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()

	NI = 3
	NJ = 1
	NK = 1

	d={'nomvar':(1,"CSV"),'etiket':(1,'CSVREADER'),'nbits':(1,24),'datyp':(1,1),'grtyp':(1,'X'),'typvar':(1,'X'),
	   'ip2':(1,0),'ip3':(1,0),'ig1':(1,0),'ig2':(1,0),'ig3':(1,0),'ig4':(1,0),'npas':(1,0),'grid':(1,'00'),'ni':(1,NI),
	   'nj':(1,NJ),'nk':(1,NK)}

	for k,v in d.items():
		assert(df[k].unique().size == v[0])
		assert(df[k].unique()[0] == v[1])
		
	assert(df.ip1.unique().size == 2)
	assert(df.ip1.unique()[0] == 1)
	assert(df.ip1.unique()[1] == 0)
	
	d1 = np.array([[11.1], [33.3], [55.5]],dtype=np.float32) 
	d2 = df.d[0]
	d3 = np.array([[77.7], [99.9], [110.11]],dtype=np.float32)
	d4 = df.d[1]

	assert(df.d.size == 2)
	assert(np.array_equal(d1,d2))
	assert(np.array_equal(d3,d4))
	

def test_12(plugin_test_dir):
	"""Test with a missing value in an array. The dataframe should not be returned. The assertion is true if the 
	   function call raises ValueError"""
	src = plugin_test_dir + 'missingvalue.new.csv'
	with pytest.raises(ValueError):
		fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
	

def test_13():
	""" Test the exception CsvFileReaderError"""
	with pytest.raises(fstpy.CsvFileReaderError):
		fstpy.CsvFileReader(path = "ome/ewqewq",encode_ip1=False)


def test_14(plugin_test_dir):
	""" Test the exception MinimalColumnsError"""
	with pytest.raises(fstpy.MinimalColumnsError):
		csv_file = fstpy.CsvFileReader(plugin_test_dir+ "minimalColumns.csv",encode_ip1=False)
		csv_file = csv_file.to_pandas()
		print(csv_file.to_string())


def test_15(plugin_test_dir):
	""" Test the exception ColumnsNotValidError"""
	with pytest.raises(fstpy.ColumnsNotValidError):
		csv_file = fstpy.CsvFileReader(path = plugin_test_dir  + "columnNotValid.csv",encode_ip1=False)
		csv_file.to_pandas()


def test_16(plugin_test_dir):
	""" Test the exception fstpy.Ip1andLevelExistsError"""
	with pytest.raises(fstpy.Ip1andLevelExistsError):
		df = fstpy.CsvFileReader(path = plugin_test_dir + "ip1andLevelExists.csv",encode_ip1=False)	
		df.to_pandas()


def test_17(plugin_test_dir):
	""" Test the exception fstpy.NomVarLengthError"""
	with pytest.raises(fstpy.NomVarLengthError):
		df = fstpy.CsvFileReader(path = plugin_test_dir + "nomVarLengthError.csv",encode_ip1=False)	
		df.to_pandas()


def test_18(plugin_test_dir):
	""" Test the exception fstpy.TypVarLengthError"""
	with pytest.raises(fstpy.TypVarLengthError):
		df = fstpy.CsvFileReader(path = plugin_test_dir + "typVarLengthError.csv",encode_ip1=False)	
		df.to_pandas()


def test_19(plugin_test_dir):
	""" Test the exception fstpy.EtiketVarLengthError"""
	with pytest.raises(fstpy.EtiketVarLengthError):
		df = fstpy.CsvFileReader(path = plugin_test_dir + "etiketVarLengthError.csv",encode_ip1=False)	
		df.to_pandas()


def test_20(plugin_test_dir):
	"""Test with space_in_the_headers.csv.Ignore the spaces in the headers. It should return a DataFrame"""
	src = plugin_test_dir + 'space_in_the_headers.csv'
	df = fstpy.CsvFileReader(path=src,encode_ip1=False).to_pandas()

	NI = 3
	NJ = 2
	NK = 1

	d={'nomvar':(1,"CSV"),'etiket':(1,'CSVREADER'),'nbits':(1,24),'datyp':(1,1),'grtyp':(1,'X'),'typvar':(1,'X'),
	   'ip2':(1,0),'ip3':(1,0),'ig1':(1,0),'ig2':(1,0),'ig3':(1,0),'ig4':(1,0),'npas':(1,0),'grid':(1,'00'),'ni':(1,NI),
	   'nj':(1,NJ),'nk':(1,NK)}

	for k,v in d.items():
		assert(df[k].unique().size == v[0])
		assert(df[k].unique()[0] == v[1])
		
	assert(df.ip1.unique().size == 2)
	assert(df.ip1.unique()[0] == 1)
	assert(df.ip1.unique()[1] == 0)

	d1 = np.array([[11.1, 22.2], [33.3, 44.4], [55.5, 66.6]],dtype=np.float32) 
	d2 = df.d[0]

	d3 = np.array([[77.7, 88.8], [99.9, 100.1], [110.11, 120.12]],dtype=np.float32)
	d4 = df.d[1]

	assert(np.array_equal(d1,d2))
	assert(np.array_equal(d3,d4))
	assert(df.d.size == 2)

	
	
		
		
	

# """ Test checkColumns Function"""
# def test_7(input_file_nbits_void,input_file_nbits_24bits):
#     csv_file2 = fstpy.CsvFileReader(path = input_file_nbits_24bits,encode_ip1=False)
#     file2_df= csv_file2.to_pandas_no_condition()
#     file2_df["dateo"] = fstpy.create_encoded_dateo(datetime.datetime.utcnow())
#     csv_file = fstpy.CsvFileReader(path = input_file_nbits_void,encode_ip1=False)
#     csv_file.to_pandas_no_condition()
#     # print(dfd.to_string())
#     csv_file.checkColumns()
#     #csv_file.change_columns_type()
#     print(csv_file.df.to_string())
#     print(csv_file2.df.to_string())
#     print(csv_file.df.dtypes)
#     assert (csv_file.df.equals(csv_file2.df))

# """ Test to_pandas Function with check columns and verify headers"""
# def test_8(input_file_nbits_void,input_file_nbits_24bits):
#     csv_file2 = fstpy.CsvFileReader(path = input_file_nbits_24bits,encode_ip1=False)
#     csv_file2.to_pandas_no_condition()
#     csv_file2.df["dateo"] = fstpy.create_encoded_dateo(datetime.datetime.utcnow())
#     csv_file = fstpy.CsvFileReader(path = input_file_nbits_void,encode_ip1=False)
#     csv_file.to_pandas()
#     print(csv_file.df)
#     print(csv_file2.df)
#     assert (csv_file.df.equals(csv_file2.df))

# def test1(plugin_test_dir):
#    src ='/home/sbf000/csv/simple_input_2level.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_1.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/simple_input_2level.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test2(plugin_test_dir):
#    src ='/home/sbf000/csv/baseTop_fileSrcGreaterEqual.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_2.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/baseTop_fileSrcGreaterEqual.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test3(plugin_test_dir):
#    src ='/home/sbf000/csv/latlon_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_3.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/latlon_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test4(plugin_test_dir):
#    src ='/home/sbf000/csv/interHeightGZ_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_4.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/interHeightGZ_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test5(plugin_test_dir):
#    src ='/home/sbf000/csv/pds1_level2_inversed_level_order.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_5.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/pds1_level2_inversed_level_order.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test6(plugin_test_dir):
#    src ='/home/sbf000/csv/baseTop_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_6.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/baseTop_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test7(plugin_test_dir):
#    src ='/home/sbf000/csv/missingvalue.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_7.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/missingvalue.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test8(plugin_test_dir):
#    src ='/home/sbf000/csv/pds2.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_8.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/pds2.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test9(plugin_test_dir):
#    src ='/home/sbf000/csv/not_all_same_number_of_items_in_lines_of_a_pds.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_9.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/not_all_same_number_of_items_in_lines_of_a_pds.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test10(plugin_test_dir):
#    src ='/home/sbf000/csv/latlonExtrapolation_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_10.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/latlonExtrapolation_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test11(plugin_test_dir):
#    src ='/home/sbf000/csv/baseTop_fileSrcLessEqual.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_11.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/baseTop_fileSrcLessEqual.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test12(plugin_test_dir):
#    src ='/home/sbf000/csv/baseTop_fileSrcEqual.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_12.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/baseTop_fileSrcEqual.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test13(plugin_test_dir):
#    src ='/home/sbf000/csv/gds1_pds1_level_glb.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_13.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/gds1_pds1_level_glb.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test14(plugin_test_dir):
#    src ='/home/sbf000/csv/baseTop_fileSrcLessThan.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_14.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/baseTop_fileSrcLessThan.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test15(plugin_test_dir):
#    src ='/home/sbf000/csv/missing_eol_at_last_line_of_data.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_15.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/missing_eol_at_last_line_of_data.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test16(plugin_test_dir):
#    src ='/home/sbf000/csv/with_space.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_16.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/with_space.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test17(plugin_test_dir):
#    src ='/home/sbf000/csv/baseTopTop_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_17.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/baseTopTop_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test18(plugin_test_dir):
#    src ='/home/sbf000/csv/baseTopBas_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_18.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/baseTopBas_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test19(plugin_test_dir):
#    src ='/home/sbf000/csv/gds1_pds1_level.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_19.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/gds1_pds1_level.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test20(plugin_test_dir):
#    src ='/home/sbf000/csv/baseTop_fileSrcNotEqual.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_20.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/baseTop_fileSrcNotEqual.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test21(plugin_test_dir):
#    src ='/home/sbf000/csv/gds1_pds1_level2.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_21.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/gds1_pds1_level2.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test22(plugin_test_dir):
#    src ='/home/sbf000/csv/latlon_fileSrc2.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_22.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/latlon_fileSrc2.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test23(plugin_test_dir):
#    src ='/home/sbf000/csv/latlongrid_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_23.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/latlongrid_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test24(plugin_test_dir):
#    src ='/home/sbf000/csv/not_all_same_number_of_lines_in_a_pds.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_24.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/not_all_same_number_of_lines_in_a_pds.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test25(plugin_test_dir):
#    src ='/home/sbf000/csv/only_1_item_per_level.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_25.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/only_1_item_per_level.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test26(plugin_test_dir):
#    src ='/home/sbf000/csv/only_1_item_per_line.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_26.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/only_1_item_per_line.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test27(plugin_test_dir):
#    src ='/home/sbf000/csv/missingvalue2.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_27.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/missingvalue2.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test28(plugin_test_dir):
#    src ='/home/sbf000/csv/latlonWithGrid_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_28.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/latlonWithGrid_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test29(plugin_test_dir):
#    src ='/home/sbf000/csv/only_1_line_per_level.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_29.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/only_1_line_per_level.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test30(plugin_test_dir):
#    src ='/home/sbf000/csv/with_comments.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_30.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/with_comments.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test31(plugin_test_dir):
#    src ='/home/sbf000/csv/inputFile.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_31.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/inputFile.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test32(plugin_test_dir):
#    src ='/home/sbf000/csv/latlonYY_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_32.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/latlonYY_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test33(plugin_test_dir):
#    src ='/home/sbf000/csv/simple_input.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_33.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/simple_input.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test34(plugin_test_dir):
#    src ='/home/sbf000/csv/baseTop_fileSrcGreaterThan.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_34.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/baseTop_fileSrcGreaterThan.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test35(plugin_test_dir):
#    src ='/home/sbf000/csv/interHeight_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_35.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/interHeight_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)


# def test36(plugin_test_dir):
#    src ='/home/sbf000/csv/latlonExtrapolation2_fileSrc.new.csv.format'
#    df = fstpy.CsvFileReader(src,encode_ip1=False).to_pandas()
#    df = fstpy.add_grid_column(df)
#    df['dateo'] =  455375527
#    df['datev'] =  455375527
#    df['etiket'] = '__CSVREAX'
#    print(df.to_string())
#    print(df.dtypes)
#    results_file = TMP_PATH + 'test_36.std'
#    fstpy.delete_file(results_file)
#    fstpy.StandardFileWriter(results_file,df).to_fst()
#    #open and read comparison file
#    file_to_compare = plugin_test_dir + '/home/sbf000/csv/latlonExtrapolation2_fileSrc.csv.std'
#    #compare results
#    res = fstcomp(results_file,file_to_compare)
#    fstpy.delete_file(results_file)
#    assert(res)






    






    
            




