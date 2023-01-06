
rm -rf /media/hd/testing/tmp_test
any2dataset --url_list freesound_parquet.parquet --input_format "parquet"\
         --url_col "download_url" --caption_col "description" --output_format webdataset\
           --output_folder /media/hd/testing/tmp_test --processes_count 16 --thread_count 64\
             --enable_wandb True