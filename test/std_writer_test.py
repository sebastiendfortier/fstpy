# -*- coding: utf-8 -*-
from numpy.lib.function_base import delete
import pytest
from rpnpy.librmn.all import FSTDError
from test import TMP_PATH, TEST_PATH
import tempfile
import fstpy.all as fstpy
pytestmark = [pytest.mark.std_writer, pytest.mark.unit_tests]


@pytest.fixture
def input_file():
    return TEST_PATH + 'ReaderStd/testsFiles/source_data_5005.std'

@pytest.fixture
def tmp_file():
    temp_name = next(tempfile._get_candidate_names())
    return TMP_PATH + temp_name
#filename:str, df:pd.DataFrame, add_meta_fields=True, overwrite=False, load_data=False

# def test_read_write_noload(input_file,tmp_file):
#     df = fstpy.StandardFileReader(input_file).to_pandas()
#     fstpy.StandardFileWriter(tmp_file,df).to_fst()
#     res = fstpy.fstcomp(input_file,tmp_file)
#     assert(res)

# def test_invalid_path(input_file):
#     std_file = StandardFileReader(input_file,load_data=True)
#     df = std_file.to_pandas()

#     #should crash
#     std_file_writer = StandardFileWriter('/tmp/123456/1',df)
#     with pytest.raises(FileNotFoundError):
#         std_file_writer.to_fst()

# def test_empty_df(tmp_file):
#     df = pd.DataFrame(dtype=object)

#     #should crash
#     with pytest.raises(StandardFileWriterError):
#         std_file_writer = StandardFileWriter(tmp_file,df)
#     #std_file_writer.to_fst()

# def test_default_not_load_datad(input_file,tmp_file):
#     std_file = StandardFileReader(input_file)
#     df = std_file.to_pandas()

#     std_file_writer = StandardFileWriter(tmp_file,df)
#     std_file_writer.to_fst()


# def test_default_not_load_datad_same_file(input_file,tmp_file):
#     file = tmp_file

#     std_file = StandardFileReader(input_file,load_data=True,query='nomvar=="TT"')
#     df = std_file.to_pandas()

#     std_file_writer = StandardFileWriter(file,df)
#     std_file_writer.to_fst()


#     std_file = StandardFileReader(file)
#     tmp_df = std_file.to_pandas()

#     std_file_writer = StandardFileWriter(file,tmp_df)
#     with pytest.raises(StandardFileError):
#         std_file_writer.to_fst()

#     delete_file(file)


# def test_default_not_load_datad_same_file_overwrite(input_file,tmp_file):
#     file = tmp_file

#     std_file = StandardFileReader(input_file,load_data=True,query='nomvar=="TT"')
#     df = std_file.to_pandas()

#     std_file_writer = StandardFileWriter(file,df)
#     std_file_writer.to_fst()

#     std_file = StandardFileReader(file)
#     tmp_df = std_file.to_pandas()

#     std_file_writer = StandardFileWriter(file,df,overwrite=True)
#     std_file_writer.to_fst()



# # def test_default_not_load_datad_same_file_overwrite(input_file,tmp_file):
# #     file = tmp_file

# #     std_file = StandardFileReader(input_file)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df,overwrite=True)
# #     std_file_writer.to_fst()

# #     std_file = StandardFileReader(tmp_file)
# #     tmp_df = std_file.to_pandas()

# #     status = fstcomp_df(df,tmp_df)
# #     assert status

# # def test_default_normal(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,load_data=True)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file,load_data=True)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_default_meta_only_load_datad(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,load_data=True,read_meta_fields_only=True)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file,load_data=True)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_default_meta_only(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,read_meta_fields_only=True)
# #     df = std_file.to_pandas()

# #     #should crash
# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()
# #     assert False

# # def test_default_no_extra(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,decode_metadata=False)
# #     df = std_file.to_pandas()

# #     #should crash
# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()
# #     assert False

# # def test_default_no_extra_load_datad(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,load_datad=True,decode_metadata=False)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file,load_data=True)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_default_no_extra_load_datad(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,load_data=True,decode_metadata=False,query='nomvar=="UU"')
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file,load_data=True)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_default_load_datad_query(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,load_data=True,query='nomvar=="UU"')
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file,load_data=True)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status


# # def test_params_normal_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df,load_data=True)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file,load_data=True)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_params_meta_only_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,read_meta_fields_only=True)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df,load_data=True)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file,load_data=True)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_params_no_extra_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,decode_metadata=False)
# #     df = std_file.to_pandas()

# #     #should crash
# #     std_file_writer = StandardFileWriter(tmp_file,df,load_data=True)
# #     std_file_writer.to_fst()
# #     assert False

# # def test_params_no_extra_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,decode_metadata=False)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df,load_data=True)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file,load_data=True)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_params_no_extra_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,decode_metadata=False,query='nomvar=="UU"')
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df,load_data=True)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file,load_data=True)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_params_query_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,query='nomvar=="UU"')
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df,load_data=True)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file,load_data=True)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status
