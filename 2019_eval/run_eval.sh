
PROTECTED_ATTR=( "gender" "country" )


for G in "${PROTECTED_ATTR[@]}";
do
	python3 trec-fair-ranking-evaluator.py  \
            --groundtruth_file TREC-Fair-Ranking-training-sample.json  \
            --query_sequence_file TREC-Sample5000.csv \
            --group_annotations_file group_annotations/article-$G.csv \
            --group_definition $G
done
