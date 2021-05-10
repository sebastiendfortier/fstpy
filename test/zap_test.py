# -*- coding: utf-8 -*-
import pytest
import fstpy
from test import TEST_PATH

pytestmark = [pytest.mark.zap, pytest.mark.regressions]


@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"Zap/testsFiles/"

# def test_regtest_1(plugin_test_dir):
#     pass
#     """Test #1 : Tester l'option --typeOfField avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --typeOfField BLABLABLA]

#     #write the result
#     results_file = TMP_PATH + "test_zap_1.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_2():
#     """Test #2 : Tester l'option --run avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --run BLABLABLA]

#     #write the result
#     results_file = TMP_PATH + "test_zap_2.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_3():
#     """Test #3 : Tester l'option --ensembleMember avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --ensembleMember BLABLABLA]

#     #write the result
#     results_file = TMP_PATH + "test_zap_3.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_5():
#     """Test #5 : Tester l'option --verticalLevel avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --verticalLevel -1]

#     #write the result
#     results_file = TMP_PATH + "test_zap_5.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_6():
#     """Test #6 : Tester l'option --verticalLevelType avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --verticalLevelType BLABLABLA]

#     #write the result
#     results_file = TMP_PATH + "test_zap_6.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_7():
#     """Test #7 : Tester l'option --forecastHour avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --forecastHour -10]

#     #write the result
#     results_file = TMP_PATH + "test_zap_7.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_8():
#     """Test #8 : Tester l'option --forecastHourOnly avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --forecastHourOnly -10]

#     #write the result
#     results_file = TMP_PATH + "test_zap_8.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_9():
#     """Test #9 : Tester l'option --userDefinedIndex avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --userDefinedIndex -10]

#     #write the result
#     results_file = TMP_PATH + "test_zap_9.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_10():
#     """Test #10 : Tester l'option --nbitsForDataStorage avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --nbitsForDataStorage i65]

#     #write the result
#     results_file = TMP_PATH + "test_zap_10.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_11():
#     """Test #11 : Tester l'option --unit avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --unit i65]

#     #write the result
#     results_file = TMP_PATH + "test_zap_11.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_12():
#     """Test #12 : Tester l'option --forecastHourOnly avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --forecastHourOnly -10:00:00]

#     #write the result
#     results_file = TMP_PATH + "test_zap_12.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_13():
#     """Test #13 : Tester l'option --forecastHourOnly avec une valeur valide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11:00:01] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_13.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_13.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_14():
#     """Test #14 : Tester l'option --forecastHour avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --forecastHour -10:00:00]

#     #write the result
#     results_file = TMP_PATH + "test_zap_14.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_15():
#     """Test #15 : Tester l'option --forecastHour avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHour 11:38:00] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_15.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_16():
#     """Test #16 : Tester l'option --forecastHour avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHour 11.633333333] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_16.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_17():
#     """Test #17 : Tester l'option --forecastHourOnly, --timeStepNumber et --lenghtOfTimeStep avec une valeur valide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.633333333 --lenghtOfTimeStep 1 --timeStepNumber 41880] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_17.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_17.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_18():
#     """Test #18 : Tester l'option --forecastHour, --timeStepNumber et --lenghtOfTimeStep avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.6 --lenghtOfTimeStep 1 --timeStepNumber 41880] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_18.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_19():
#     """Test #19 : Tester l'option --forecastHour, --timeStepNumber et --lenghtOfTimeStep avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.633333333 --lenghtOfTimeStep 2 --timeStepNumber 41880] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_19.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_20():
#     """Test #20 : Tester l'option --forecastHour, --timeStepNumber et --lenghtOfTimeStep avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.633333333 --lenghtOfTimeStep 1 --timeStepNumber 41888] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_20.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_21():
#     """Test #21 : Tester l'option --forecastHour et --lenghtOfTimeStep avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.633333333 --lenghtOfTimeStep 1] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_21.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_22():
#     """Test #22 : Tester l'option --forecastHour et --timeStepNumber avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11.633333333 --timeStepNumber 41880] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_22.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_23():
#     """Test #23 : Tester l'option --forecastHourOnly, --timeStepNumber et --lenghtOfTimeStep avec une valeur valide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11:31:00 --lenghtOfTimeStep 1 --timeStepNumber 41460] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_23.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_23.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_24():
#     """Test #24 : Tester l'option --forecastHourOnly, --timeStepNumber et --lenghtOfTimeStep avec une valeur valide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11:31:00 --lenghtOfTimeStep 60 --timeStepNumber 691] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_24.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_24.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_25():
#     """Test #25 : Tester l'option --forecastHourOnly, --timeStepNumber et --lenghtOfTimeStep avec une valeur valide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --forecastHourOnly 11:31:01 --lenghtOfTimeStep 1 --timeStepNumber 41461] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_25.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_25.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_26():
#     """Test #26 : Tester l'option --modificationFlag, avec 2 valeurs valide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=TRUE,BOUNDED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_26.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_26.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_27():
#     """Test #27 : Tester l'option --modificationFlag, avec 1 valeur valide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_27.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_27.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_28():
#     """Test #28 : Tester l'option --modificationFlag, avec 1 valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPEDS=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_28.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_29():
#     """Test #29 : Tester l'option --modificationFlag, avec 1 valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=TRU] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_29.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_30():
#     """Test #30 : Tester l'option --modificationFlag, avec FILTERED=TRUE!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag FILTERED=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_30.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_30.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_31():
#     """Test #31 : Tester l'option --modificationFlag, avec INTERPOLATED=TRUE!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag INTERPOLATED=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_31.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_31.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_32():
#     """Test #32 : Tester l'option --modificationFlag, avec UNITCONVERTED=TRUE!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag UNITCONVERTED=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_32.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_32.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_33():
#     """Test #33 : Tester l'option --modificationFlag, avec ALL_FLAGS=TRUE!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ALL_FLAGS=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_33.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_33.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_34():
#     """Test #34 : Tester l'option --modificationFlag, avec ZAPPED!"""
#     # open and read source
#     source0 = plugin_test_dir + "resulttest_26.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_34.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_34.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_35():
#     """Test #35 : Tester l'option --modificationFlag, avec ZAPPED and FILTERED!"""
#     # open and read source
#     source0 = plugin_test_dir + "resulttest_30.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=FALSE,FILTERED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_35.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_34.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_36():
#     """Test #36 : Tester l'option --modificationFlag, avec FILTERED!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag FILTERED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_36.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_34.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_37():
#     """Test #37 : Tester l'option --modificationFlag, avec INTERPOLATED!"""
#     # open and read source
#     source0 = plugin_test_dir + "resulttest_31.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag INTERPOLATED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_37.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_34.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_38():
#     """Test #38 : Tester l'option --modificationFlag, avec UNITCONVERTED!"""
#     # open and read source
#     source0 = plugin_test_dir + "resulttest_32.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag UNITCONVERTED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_38.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_34.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_39():
#     """Test #38 : Tester l'option --modificationFlag, avec ZAP et FILTERED!"""
#     # open and read source
#     source0 = plugin_test_dir + "zap_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ZAPPED=TRUE,FILTERED=TRUE] >>', '[Zap --modificationFlag ZAPPED=FALSE,FILTERED=FALSE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_39.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_34.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_40():
#     """Test #40 : Tester l'option --modificationFlag, avec ENSEMBLEEXTRAINFO!"""
#     # open and read source
#     source0 = plugin_test_dir + "resulttest_32.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute Zap
#     df = Zap(src_df0).compute()
#     #['[ReaderStd --input {sources[0]}] >>', '[Zap --modificationFlag ALL_FLAGS=FALSE] >>', '[Zap --modificationFlag ENSEMBLEEXTRAINFO=TRUE] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_zap_40.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_40.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


