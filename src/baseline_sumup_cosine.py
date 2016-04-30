








def load_word2vec():
    word2emb={}
    readfile=open('/mounts/data/proj/wenpeng/Dataset/word2vec_words_300d.txt', 'r')
    for line in readfile:
        parts=line.strip().split()
        word2emb[parts[0]]=map(float, parts[1:])
    readfile.close()
    return word2emb

def cosine_sumup():
    read_testfile=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/annotated_fb_data_test_PNQ_50nega_str&des.txt', 'r')
    for line in read_testfile:
        parts=line.strip().split('\t')
        
        

if __name__ == '__main__':
    
    
    
    
    
    
    lis=['0.45', '0.98', '8.91']
    print map(float, lis[1:])

