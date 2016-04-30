import gzip
import nltk
import string
import codecs

def last_slash_pos(str):
    return str.rfind('/')
def last_dot_pos(str):
    return str.rfind('.')
def extract_related_triples():
    #load freebase2M as map
    head2tripleSet={}
    read_freebase=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/freebase-subsets/freebase-FB2M-ungrouped.txt', 'r')
    count=0
    for line in read_freebase:
        parts=line.strip().split()
        head=parts[0][last_slash_pos(parts[0])+1:]
        relation=parts[1][last_slash_pos(parts[1])+1:]
        tail=parts[2][last_slash_pos(parts[2])+1:]
        tripleSet=head2tripleSet.get(head)
        if tripleSet is None:
            tripleSet=set()           
        tripleSet.add((head, relation, tail))
        head2tripleSet[head]=tripleSet
#         print head2tripleList
#         exit(0)
        count+=1
    read_freebase.close()
    print 'Freebase has totally', count, 'triples'
    
    #extend SimpleQuestion by negative triples
    SQs=['annotated_fb_data_train', 'annotated_fb_data_test', 'annotated_fb_data_valid']
    max_triples=0
    min_triples=10000
    
    for SQ in SQs:
        zero_nega=0
        read_SQ=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/'+SQ+'.txt', 'r')
        write_SQ=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/'+SQ+'_PNQ.txt', 'w')
        for line in read_SQ:
            parts=line.strip().split('\t')
            Q=parts[3]
            head=parts[0][last_slash_pos(parts[0])+1:]
            relation=parts[1][last_slash_pos(parts[1])+1:]
            tail=parts[2][last_slash_pos(parts[2])+1:]       
            
            tripleSet=head2tripleSet.get(head)
            posi=(head, relation, tail)
            count=0
            if tripleSet is None: # no negative triples
                zero_nega+=1
                print head
            else:
                if posi in tripleSet:
                    tripleSet.remove(posi)
                if len(tripleSet)>0:
                    write_SQ.write(head+' == '+relation+' == '+tail+'\t')
                    count=len(tripleSet)
                    for triple in tripleSet:
                        if triple != posi:
                            write_SQ.write(triple[0]+' == '+triple[1]+' == '+triple[2]+'\t')
                    write_SQ.write(Q+'\n')
            
                    if count>max_triples:
                        max_triples=count
                    if count<min_triples:
                        min_triples=count
        read_SQ.close()
        write_SQ.close()
        print 'PNQ reformat over, max_nega_triples:', max_triples, 'min_nega_triples: ', min_triples, 'remove zero nega:', zero_nega   
            
def make_the_same_size_of_nega():
    #remove those with no negative triples
    path='/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/'
    files=['annotated_fb_data_train_PNQ', 'annotated_fb_data_test_PNQ', 'annotated_fb_data_valid_PNQ']  
    nega_size=   5+2 # 50 nega and one posi, one question
    for fil in files:
        readfile=open(path+fil+'.txt', 'r')
        writefile=open(path+fil+'_'+str(nega_size-2)+'nega.txt', 'w')
        for line in readfile:
            parts=line.strip().split('\t')
            length=len(parts)
            if length>2:
                if length<nega_size:
                    repeat_triple=parts[-2]
                    for triple in parts[:-1]:
                        writefile.write(triple+'\t')
                    for i in range(nega_size-length):
                        writefile.write(repeat_triple+'\t')

                else:
                    for triple in parts[:nega_size-1]:
                        writefile.write(triple+'\t')

                writefile.write(parts[-1]+'\n')#question
        writefile.close()
        readfile.close()
    print 'over'
def    load_id2names():
                readfile=open('/mounts/data/corp/freebase.com/freebase.id2names', 'r')
                id2names={}
                count=0
                for    line    in    readfile:
                                parts=line.strip().split('::')
                                id2names[parts[0].strip()]=parts[1].strip()
                                count+=1
                                #exit(0)
                print count, 'names, loaded over'
                readfile.close()
                return id2names
            
def create_id_to_name_des_types():
    id2names=load_id2names()
    id2des=entity_description_statistics()
    id2types=load_id2types()
    ids=set(id2names.keys())|set(id2des.keys())|set(id2types.keys())
    writefile=gzip.open('/mounts/data/proj/wenpeng/Dataset/freebase/id_to_name_des_types.txt.gz', 'w')
    
    for id in ids:
        types=id2types.get(id, '<None>')
        name=id2names.get(id, '<None>')
        des=id2des.get(id, '<None>')
        writefile.write(id+'\t'+name+'\t'+des)
        for type in types:
            writefile.write('\t'+type)
        writefile.write('\n')
    writefile.close()
    print 'create_id_to_name_des_types store over.'

def load_id2types():
    readfile=open('/mounts/data/corp/freebase.com/freebase.id2types', 'r')
    id2types={}
    discard_type='common.topic'
    for line in readfile:
        parts=line.strip().split('::')
        types=set()
        id=parts[0].strip()
        for part in parts[1:]:
            part=part.strip()
            types.add(part[last_slash_pos(part)+1:])
#             print types
#             exit(0)
        if len(types)>1 and discard_type in types:
            types.discard(discard_type)
        id2types[id]=types
            
    print 'types store over.'
    readfile.close()
    return id2types    
    
    
def load_id2notabletypes():
    readfile=gzip.open('/mounts/data/corp/freebase.com/freebase-rdf-2014-04-13-00-00.gz', 'r')
    id2notabletype={}
    
    for line in readfile:
        parts=line.strip().split()
        if parts[0].find('/m.')>0 and parts[1].find('/type.object.type')>0:
            id=parts[0][last_slash_pos(parts[0].strip())+1:-1]
            type=parts[2][last_slash_pos(parts[2].strip())+1:-1]
            id2notabletype[id]=type
    print 'notable typle store over.'
    readfile.close()
    return id2notabletype

def entity_description_tokenize():
    des_file=gzip.open('/mounts/data/proj/wenpeng/Dataset/freebase/Heike_id2des.txt.gz', 'r')
    write_file= gzip.open('/mounts/data/proj/wenpeng/Dataset/freebase/Heike_id2des_tokenized.txt.gz', 'w')    
    for line in des_file:
        parts=line.strip().split('\t')
        if len(parts)==2:
            tokenized_des=nltk.word_tokenize(parts[1].strip().lower().decode('utf-8'))
            write_file.write(parts[0].strip()+'\t'+' '.join([x.encode('utf-8') for x in tokenized_des])+'\n')
    write_file.close()
    des_file.close()
    
def entity_description_statistics():
    #first load all descriptions
    des_file=gzip.open('/mounts/data/proj/wenpeng/Dataset/freebase/Heike_id2des_tokenized.txt.gz', 'r')
    id2des={}
    for line in des_file:

        parts=line.strip().split('\t')
        if len(parts)==2:
            id2des[parts[0].strip()]=parts[1].strip()
    des_file.close()
    print 'totally id2des size:', len(id2des)
    return id2des
def idList2StrDndDes(ids, id2names, id2des):
    strList=[]
    desList=[]
    for id in ids:
        str=id2names.get(id)
        if str is None:
            str=id
        des=id2des.get(id)
        if des is None:
            des=str     
        strList.append(str)
        desList.append(des)
    return strList, desList
    

def MID2str_str2des():
    #first load MID2str, and str to des
    nega_size=5
    id2names=load_id2names()
    id2des=entity_description_statistics()
    path='/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/'
    files=['annotated_fb_data_train_PNQ_'+str(nega_size)+'nega', 'annotated_fb_data_test_PNQ_'+str(nega_size)+'nega', 'annotated_fb_data_valid_PNQ_'+str(nega_size)+'nega']      
    for fil in files:
        readfile=open(path+fil+'.txt', 'r')
        writefile=open(path+fil+'_str&des.txt', 'w')
        for line in readfile:
            parts=line.strip().split('\t')

            ids=[]

            for i in range(nega_size+1): #1 posi, 50 nega
                ids.append('m.'+parts[i].split(' == ')[0].strip())
                ids.append('m.'+parts[i].split(' == ')[2].strip())
            strList, desList=idList2StrDndDes(ids, id2names, id2des) #52
            for i in range(len(parts[:-1])):#51
                units=parts[i].split(' == ')
                relation=units[1]
                writefile.write(strList[i*2]+' == '+relation+' == '+strList[i*2+1]+'\t')
            for des in desList: #52, 1 head, 51 tail
                writefile.write(des+'\t')
            writefile.write(parts[-1]+'\n')
        writefile.close()
        readfile.close()
        print fil, '..finished'
            
def freebase_id2des():
    readfile=gzip.open('/mounts/data/corp/freebase.com/freebase.descriptions.gz', 'r')
    writefile=gzip.open('/mounts/data/proj/wenpeng/Dataset/freebase/Heike_id2des.txt.gz', 'w')
    for line in readfile:
        if line.strip().find('@en')>=0:
#             print line
#             exit(0)
            parts=line.strip().split('\t')
            id= parts[0][last_slash_pos(parts[0])+1:-1]
            posi=parts[2].find('"')
            rposi=parts[2].rfind('"')
            des= parts[2][posi+1:rposi]
            writefile.write(id+'\t'+des+'\n')
    writefile.close()
    readfile.close()
    print 'id2des over'                   

def ungroup_FB2M5M():
    read5M=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/freebase-subsets/freebase-FB5M.txt', 'r')
    write5M=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/freebase-subsets/freebase-FB5M-ungrouped.txt', 'w')
    fb5M=set()
    for line in read5M:
        parts=line.strip().split()
        size=len(parts)-2
        for i in range(size):
            new_triple=parts[0]+'\t'+parts[1]+'\t'+parts[i]
            if  new_triple not in fb5M and parts[i].find('/m/')>=0:
                fb5M.add(new_triple)
                write5M.write(new_triple+'\n')
    write5M.close()
    read5M.close()
    print 'ungroup finished'

def check_if_fb5M_contains_fb2M():
    read5M=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/freebase-subsets/freebase-FB5M-ungrouped.txt', 'r')
    fb5M_entity=set()
    fb5M_relation=set()
    for line in read5M:
        parts=line.strip().split()
        fb5M_entity.add(parts[0])
        fb5M_entity.add(parts[2])
        fb5M_relation.add(parts[1])
    read5M.close()
    print '5M loaded over'
    read2M=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/freebase-subsets/freebase-FB2M-ungrouped.txt', 'r')
    e_co=0
    r_co=0
    for line in read2M:
        parts=line.strip().split()
        if parts[0] not in fb5M_entity:
            e_co+=1
        if parts[2] not in fb5M_entity:
            e_co+=1
        if parts[1] not in fb5M_relation:
            r_co+=1
    read2M.close()
    print e_co, r_co

def combine_fb2M_fb5M():
    read5M=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/freebase-subsets/freebase-FB5M-ungrouped.txt', 'r')
    writefile=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/freebase-subsets/freebase-FB5M2M-combined.txt', 'w')
    fb5M=set()

    for line in read5M:
        writefile.write(line.strip()+'\n')
        fb5M.add(line.strip())
    read5M.close()
    print '5M loaded over'
    read2M=open('/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/freebase-subsets/freebase-FB2M-ungrouped.txt', 'r')

    for line in read2M:
        if line.strip() not in fb5M:
            writefile.write(line.strip()+'\n')
    read2M.close()
    print 'over'



def split_Questions_into_mention_remainQ():
    path='/mounts/data/proj/wenpeng/Dataset/freebase/SimpleQuestions_v2/'
    infile=['annotated_fb_data_train_PNQ_50nega_str&des.txt', 'annotated_fb_data_valid_PNQ_50nega_str&des.txt', 'annotated_fb_data_test_PNQ_50nega_str&des.txt']
    outfile=['annotated_fb_data_train_mention_remainQ.txt', 'annotated_fb_data_valid_mention_remainQ.txt', 'annotated_fb_data_test_mention_remainQ.txt']
    exclude= set(string.punctuation)
    for i in range(3):
        readfile=codecs.open(path+infile[i], 'r', 'utf-8')
        writefile=codecs.open(path+outfile[i], 'w', 'utf-8')
        line_co=0
        for line in readfile:
            mention=''
            
            parts=line.strip().split('\t')
            pos_triple=parts[0]
            head=pos_triple.strip().split(' == ')[0].strip().lower()
            raw_Q=parts[-1].lower()
            Q=nltk.word_tokenize(raw_Q.strip())
            Q_words=set(Q)
            
            remainQ=Q
#             print 'raw_Q', raw_Q
#             print 'Q', Q

            head_words=head.split()
            for head_word in head_words:
                if head_word in Q_words:
                    mention+=' '+head_word
#                     print 'head_word:', head_word, 'remainQ:', remainQ
                    Q_words.discard(head_word)
                    remainQ.remove(head_word)
            if remainQ[-1] in exclude:
                remainQ=remainQ[:-1]     
            if len(mention.strip())==0:
                mention=head
#                 exit(0)        
#             print head, raw_Q, mention, remainQ
            writefile.write(head+'\t'+raw_Q+'\t'+mention.strip()+'\t'+' '.join(remainQ)+'\n')
        readfile.close()
        writefile.close()
        print i, 'finished'
                    
            
            
            
            





if __name__ == '__main__':
#     entity_description_tokenize()
#     extract_related_triples()
#     make_the_same_size_of_nega()
    MID2str_str2des()

#     freebase_id2des()
#     create_id_to_name_des_types()
#     check_if_fb5M_contains_fb2M()
#     ungroup_FB2M5M()
#     combine_fb2M_fb5M()
#     split_Questions_into_mention_remainQ()
        
        
        
        
    