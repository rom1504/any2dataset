set -e
rm -rf bench
any2dataset --url_list test_10000.parquet --input_format "parquet"\
         --url_col "download_url" --caption_col "description" --output_format webdataset\
           --output_folder bench --processes_count 16 --thread_count 64\
             --enable_wandb True
#rm -rf bench