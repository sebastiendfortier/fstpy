# -*- coding: utf-8 -*-
import pytest
import fstpy
from test import TEST_PATH


pytestmark = [pytest.mark.std_writer_regtests, pytest.mark.regressions]

@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"WriterStd/testsFiles/"

##### ajouter un test
##### tester des nans a l'ecriture


# def test_dump():
#     pass
# def test_regtest_1():
    # pass
#     """Test #1 : Tester l'option --output avec un path qui n'existe pas!"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output /tmp//toto81mqcM/toto.std]

#     #write the result
#     results_file = TMP_PATH + "test_write_1.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_2():
#     """Test #2 : Tester l'option --output avec un path qui existe mais qui est un nom de fichier!"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output /tmp//totoGK6msl/bidon/toto.std]

#     #write the result
#     results_file = TMP_PATH + "test_write_2.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_3():
#     """Test #3 : Tester l'option --output avec un path existant qui est un répertoire mais dont on n'a pas les permissions!"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output /media/toto.std]

#     #write the result
#     results_file = TMP_PATH + "test_write_3.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_4():
#     """Test #4 : Tester l'option --writingMode avec une valeur invalide!"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output /tmp/toto --writingMode TOTO]

#     #write the result
#     results_file = TMP_PATH + "test_write_4.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_5():
#     """Test #5 : Tester l'option --writingMode avec la valeur NOPREVIOUS et un fichier de sortie existant. Il doit indiquer que le fichier d'output existe déjà!"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output /tmp//totoYLLxnu/toto.std --writingMode NOPREVIOUS]

#     #write the result
#     results_file = TMP_PATH + "test_write_5.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_6():
#     """Test #6 : Tester l'option --writingMode avec la valeur NOPREVIOUS et un fichier de sortie inexistant. Aucun message d'erreur doit apparaître, donc tout devra fonctionner normalement!"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --writingMode NOPREVIOUS --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_6.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + ""

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_7():
#     """Test #7 : Tester l'option --writingMode avec la valeur NEWFILEONLY et un fichier de sortie inexistant. Aucun message d'erreur doit apparaître, donc tout devra fonctionner normalement!"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --writingMode NEWFILEONLY --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_7.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + ""

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_8():
#     """Test #8 : Tester l'option --writingMode avec la valeur NEWFILEONLY et un fichier de sortie existant. Un message d'avertissement doit apparaître et tout devra fonctionner normalement!"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --writingMode NEWFILEONLY --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_8.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + ""

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_9():
#     """Test #9 : Tester la partie pdsLabel de l'etiket. Comme le pdsLabel SHORT a seulement 5 caractèresun caractère _ sera ajouter pour que cette partie soit de longueur 6."""
#     # open and read source
#     source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --pdsLabel SHORT --doNotFlagAsZapped] >>[WriterStd --output {destination_path} ]

#     #write the result
#     results_file = TMP_PATH + "test_write_9.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "NEW/UUVV5x5_extended_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_10():
#     """Test #10 : Test que la partie implementation de l'etiket est bien écrit avec la bonne valeur OPERATIONAL = N, PARALLEL = P et EXPERIMENTAL = X"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> ( ([Select --fieldName UU] >> [Zap --implementation OPERATIONAL --doNotFlagAsZapped]) + ([Select --fieldName VV] >> [Zap --implementation PARALLEL --doNotFlagAsZapped]) + ([Select --fieldName TT] >> [Zap --implementation EXPERIMENTAL --doNotFlagAsZapped]) ) >> [WriterStd --output {destination_path} ]

#     #write the result
#     results_file = TMP_PATH + "test_write_10.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "UUVVTT5x5_implementationRR_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_11():
#     """Test #11 : Test que la partie ensemble member de l'etiket est bien écrit avec la bonne valeur"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --ensembleMember 077 --doNotFlagAsZapped] >> [WriterStd --output {destination_path} ]

#     #write the result
#     results_file = TMP_PATH + "test_write_11.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "UUVVTT5x5_ensemble_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_12():
#     """Test #12 : Test que la partie run de l'etiket est bien écrit avec la bonne valeur"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --run G3 --doNotFlagAsZapped] >> [WriterStd --output {destination_path} ]

#     #write the result
#     results_file = TMP_PATH + "test_write_12.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "UUVVTT5x5_run_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_13():
#     """Test #13 : Test la lecture d'un fichier très simple, 1 grille et 2 champs"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_13.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + ""

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_14():
#     """Test #14 : Test la lecture d'un fichier complexe, plusieurs grilles et plusieurs champs"""
#     # open and read source
#     source0 = plugin_test_dir + "input_big_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_14.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + ""

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_15():
#     """Test #15 : Test la lecture d'un fichier de modèle en pression"""
#     # open and read source
#     source0 = plugin_test_dir + "input_model"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [Select --fieldName UU,VV,TT] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_15.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "sigma12000_pressure_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_16():
#     """Test #16 : Test la clé paramétrable --noMetadata. Le fichier résultant devrait contenir seulement les champs de données. Les tictic tactac ne seront pas écrit."""
#     # open and read source
#     source0 = plugin_test_dir + "input_big_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --noMetadata --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_16.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "input_big_noMeta_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_17():
#     """Test #17 : Test la clé paramétrable --metadataOnly. Le fichier résultant devrait contenir seulement les champs de metadata"""
#     # open and read source
#     source0 = plugin_test_dir + "input_big_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --metadataOnly --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_write_17.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "input_big_metaOnly_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_18():
#     """Test #18 : Test la lecture d'un fichier qui contiendrait plusieurs grilles. Le fichier écrit contiendra une seule grille et les champs seront combinés."""
#     # open and read source
#     source0 = plugin_test_dir + "fstdWithDuplicatedGrid_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_18.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "readDuplicatedGrid_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_19():
#     """Test #19 : Test la lecture et la réécriture d'un champ(!!) 64 bits"""
#     # open and read source
#     source0 = plugin_test_dir + "tt_stg_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_write_19.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + ""

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_20():
#     """Test #20 : Tester avec un champ qui a un pdsName plus grand que 4! Un message d'erreur indiquant que le pds n'a pas pu être enregistré car la longueur du _pdsName est trop grande pour les fichiers standards."""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [ZapSmart --fieldNameFrom UU --fieldNameTo UUUUU] >> [WriterStd --output /tmp/toto.std --noUnitConversion]

#     #write the result
#     results_file = TMP_PATH + "test_write_20.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_21():
#     """Test #21 : Tester avec un champ qui a un pdsName égale à 4! Aucun message d'erreur doit apparaître, donc tout devra fonctionner normalement!"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [ZapSmart --fieldNameFrom UU --fieldNameTo UUUU] >> [WriterStd --output {destination_path} --ignoreExtended --noUnitConversion --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_21.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "fieldName4characters_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_22():
#     """Test #22 : Tester avec un forecast hour qui devra être arrondit à l'entier supérieur Forecast hour arrondit à l'entier supérieur et fichier écrit sans problème"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [Zap --forecastHour 10.6] >> [WriterStd --output {destination_path} --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_write_22.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_23():
#     """Test #23 : Tester avec un forecast hour qui devra être arrondit à l'entier inférieur Forecast hour arrondit à l'entier inférieur et fichier écrit sans problème"""
#     # open and read source
#     source0 = plugin_test_dir + "inputFile.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [Zap --forecastHour 10.4] >> [WriterStd --output {destination_path} --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_write_23.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_24():
#     """Test #24 : Test un pds_label plus grand que 6 mais avec implementation = 'EXPERIMENTAL'. Doit passer mais l'étiquette sera tronqué à 6 caractères."""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --pdsLabel ABCDEFG --implementation EXPERIMENTAL] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_24.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "UUVVTT5x5_pdsLabel_egale_a_7_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_25():
#     """Test #25 : Test un pds_label plus grand que 6 mais avec implementation = 'OPERATIONAL'. L'écriture ne doit pas fonctionnée."""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --pdsLabel ABCDEFG] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_25.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_26():
#     """Test #26 : Test un pds_label égale à 6 et implementation = 'OPERATIONAL'. Ce test doit fonctionné."""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [Zap --pdsLabel ABCDEF] >> [WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_write_26.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "UUVVTT5x5_pdsLabel_egale_a_7_N_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_27():
#     """Test #27 : Test lecture ecriture d'une grille #Ce test doit fonctionné."""
#     # open and read source
#     source0 = plugin_test_dir + "dm2011042100-00-00_000_dieses_no_toctoc"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path} ]

#     #write the result
#     results_file = TMP_PATH + "test_write_27.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "dieses_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_28():
#     """Test #28 : Tester l'option --writingMode avec la valeur APPEND et un fichier de sortie déjà existant. Aucun message d'erreur doit apparaître, le contenue de la mémoire est ajouté au fichier"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path}] >> [WriterStd --output {destination_path} --writingMode APPEND]

#     #write the result
#     results_file = TMP_PATH + "test_write_28.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + ""

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_29():
#     """Test #29 : Test que la partie implementation de l'etiket est bien écrit avec la bonne valeur OPERATIONAL = N, PARALLEL = P et EXPERIMENTAL = X et que la run est R1."""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> ( ([Select --fieldName UU] >> [Zap --implementation OPERATIONAL --doNotFlagAsZapped]) + ([Select --fieldName VV] >> [Zap --implementation PARALLEL --doNotFlagAsZapped]) + [Select --fieldName TT] ) >> [WriterStd --output {destination_path} ]

#     #write the result
#     results_file = TMP_PATH + "test_write_29.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "UUVVTT5x5_implementationRRa_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_30():
#     """Test #30 : Test que la partie implementation de l'etiket est bien écrit avec la bonne valeur OPERATIONAL = N, PARALLEL = P et EXPERIMENTAL = X et que la run est R1."""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> ( ([Select --fieldName UU] >> [Zap --implementation OPERATIONAL --doNotFlagAsZapped]) + ([Select --fieldName VV] >> [Zap --implementation PARALLEL --doNotFlagAsZapped]) + [Select --fieldName TT] ) >> [WriterStd --output {destination_path} ]

#     #write the result
#     results_file = TMP_PATH + "test_write_30.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "UUVVTT5x5_implementationRRa_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_32():
#     """Test #32 : Teste la lecture suivi de l'écriture avec un fichier qui contient des IP's encodés."""
#     # open and read source
#     source0 = plugin_test_dir + "FichierStandardAvecDifferendKind_file2cmp.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --encodeIP2andIP3 --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_write_32.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + ""

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_33():
#     """Test #33 : Teste la lecture suivi de l'écriture avec un fichier qui contient des IP's encodés mais qui seront pas encodés."""
#     # open and read source
#     source0 = plugin_test_dir + "FichierStandardAvecDifferendKind_file2cmp.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_write_33.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + ""

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_34():
#     """Test #34 : Teste l'écriture avec noModificationFlag"""
#     # open and read source
#     source0 = plugin_test_dir + "FichierStandardAvecDifferendKind_file2cmp.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute WriterStd
#     df = WriterStd(src_df0).compute()
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path} --noModificationFlag]

#     #write the result
#     results_file = TMP_PATH + "test_write_34.std"
#     StandardFileWriter(results_file, df, erase=True).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + ""

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


