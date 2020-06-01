######################################################
### Time of Day Recommender Parallel #################
# Recommending Time of Day to Users/Items ############
# RecSys '20 Submission Source Code ##################
######################################################

## Libraries
library(readr)
library(lubridate)
library(tidyverse)
library(reshape2)
library(foreach)
library(doParallel)

## Expecting Dataset D 
colnames(D) = c("user","item","rating","timestamp")


## Data Manipulations
D = as_tibble(D)
D = D %>% mutate(date = as.POSIXct(timestamp, origin = "1970-01-01"), hour = hour(date))

## User modeling or item modeling
model_user = F
model_item = !model_user

## Taking only relevant parts
if(model_item){
  D_feature_hour = D %>% select(item, hour)
}
if(model_user){
  D_feature_hour = D %>% select(user, hour)  
}

## Remove NAs from reduced dataset
D_feature_hour = D_feature_hour %>% na.omit()
D_feature_hour %>% dim()

## Data Split ##
message("Splitting data...")
dataset = D_feature_hour
source('GitHub/RecommenderSystems/Handling Large Data/general_cross_validation.R')
test_sets = list(D1_test, D2_test, D3_test, D4_test, D5_test)
train_sets = list(D1_train, D2_train, D3_train, D4_train, D5_train)

## Pick a similarity measure (listed)
similarity = "jaccard" # cosine, pearson, jaccard
shift_similarity = T; max_scale = T


num_cores = detectCores()
shard_multiplier = 30

pred_df_total = c()
start = Sys.time()
for(t in 1:length(test_sets)){
  
  message("Test set: ", t, "/", length(test_sets))
  D_feature_hour_train = train_sets[[t]] 
  D_feature_hour_test = test_sets[[t]] 
  
  ## Reshaping
  message("Beginning reshaping...")
  if(model_user){D_feature_hour_matrix = D_feature_hour_train %>% dcast(user ~ hour)}
  if(model_item){D_feature_hour_matrix = D_feature_hour_train %>% dcast(item ~ hour)}
  
  D_feature_hour_matrix = D_feature_hour_matrix[,-1]
  D_feature_hour_matrix = as.matrix(D_feature_hour_matrix)
  
  ## Compute Similarity
  message("Reshaping complete.")
  message("Beginning Similarity computation...")
 
  new_cols = as.character(seq(0,23,1))[which(!(as.character(seq(0,23,1)) %in% colnames(D_feature_hour_matrix)))]
  
  if(length(new_cols) > 0){
    new_data = matrix(data = 0, nrow = nrow(D_feature_hour_matrix), ncol = length(new_cols))
    colnames(new_data) = as.character(new_cols)
    D_feature_hour_matrix = cbind(D_feature_hour_matrix, new_data)
  }
  
  stopifnot(ncol(D_feature_hour_matrix) == 24)
  
  if(similarity == "jaccard"){
    message("Using ", similarity, " similarity measure.")
    sim = jaccard_similarity(t(D_feature_hour_matrix))
  }
  
  if(similarity == "cosine" & shift_similarity == TRUE){
    message("Using ", similarity, " similarity measure.")
    message("Shift similarity is on.")
    
    sim = cosine_similarity(t(D_feature_hour_matrix))
    eps = 0.01
    
    if(any(sim[!is.na(sim)] < 0)){sim = sim + abs(min(sim, na.rm = T)) + eps}
    sim[is.na(sim)] = 0 # this might make some bad behavior if there are actually negative values
    
    if(max_scale){
      sim = sim/max(sim)    
    }
    
  }
  
  if(similarity == "pearson" & shift_similarity == TRUE){
    message("Using ", similarity, " similarity measure.")
    message("Shift similarity is on.")
    
    sim = cor(D_feature_hour_matrix)
    eps = 0.01
    
    if(any(sim[!is.na(sim)] < 0)){sim = sim + abs(min(sim, na.rm = T)) + eps}
    sim[is.na(sim)] = 0
    
    if(max_scale){
      sim = sim/max(sim)    
    }
    
  }
  
  message("Similarity computation complete.")
  
  K = 1:nrow(sim) 
  pred_df_t = c()
  hours = 0:23
  
  message("Data successfully prepared.")
  message("Beginning run...")
  message(Sys.time())
  start_inner = Sys.time()  
  
  if(model_user){
    message("Unique elements: ", length(unique(D_feature_hour_train$user)))
    num_shards = num_cores*shard_multiplier
    shards = chunk(vector = unique(D_feature_hour_train$user), num_splits = num_shards)
    names(shards) = seq(1,num_shards,1)

    cl = makePSOCKcluster(num_cores)
    registerDoParallel(cl = cl)
 
    pred_df = foreach(p = 1:num_shards, 
                      .combine = rbind, 
                      .packages = c("tidyverse","lubridate","reshape2")) %dopar% {  
                        
                        D_pred = c()                
                        shard_subset = shards[[p]]                  
                        for(i in 1:length(shard_subset)){
                          user_i = shard_subset[i]
                          pred_df = data.frame(user = user_i, hour = rep(hours,each = length(K)), K = rep(K,length(K)), pred = NA)
                          
                          neighboring_items = D_feature_hour_train %>% 
                            filter(user == user_i) %>% rowwise() %>% unique()
                          
                          for(j in 1:nrow(pred_df)){
                            
                            hour_j = pred_df[j,"hour"]
                            K_j = pred_df[j, "K"]
                            
                            nbhd_sim = neighboring_items %>% 
                              mutate(item_sim = sim[as.character(hour),as.character(hour_j)]) %>% 
                              ungroup() %>% 
                              arrange(desc(item_sim)) 
                            
                            prediction = nbhd_sim %>% top_n(n = K_j, wt = item_sim) %>% summarize(pred = mean(item_sim)) %>% .$pred
                            pred_df[j,"pred"] = prediction
                            
                          }
                          
                          D_pred = bind_rows(D_pred, pred_df)
                        }

                        D_pred
                        
                      }
    
    stopCluster(cl = cl)
    
    pred_df = pred_df %>% mutate(test_set = t)

    pred_df = left_join(pred_df, 
                        D_feature_hour_test %>% unique() %>% mutate(actual = 1), 
                        by = c("user" = "user", 
                               "hour" = "hour")) %>% mutate(ae = actual - pred) 
    
    pred_df_t = bind_rows(pred_df_t, pred_df)
    
  }
  if(model_item){
    message("Unique elements: ", length(unique(D_feature_hour_train$item)))
    num_shards = num_cores*shard_multiplier
    shards = chunk(vector = unique(D_feature_hour_train$item), num_splits = num_shards)
    names(shards) = seq(1,num_shards,1)

    cl = makePSOCKcluster(num_cores)
    registerDoParallel(cl = cl)
    
    pred_df = foreach(p = 1:num_shards, 
                      .combine = rbind, 
                      .packages = c("tidyverse","lubridate","reshape2")) %dopar% {  
                        
                        D_pred = c()                
                        shard_subset = shards[[p]]                  
                        for(i in 1:length(shard_subset)){
                          item_i = shard_subset[i] 
                          pred_df = data.frame(item = item_i, hour = rep(hours,each = length(K)), K = rep(K,length(K)), pred = NA)
                          
                          neighboring_items = D_feature_hour_train %>% filter(item == item_i) %>% 
                            unique() %>% rowwise() 
                          
                          for(j in 1:nrow(pred_df)){
                            
                            hour_j = pred_df[j,"hour"]
                            K_j = pred_df[j, "K"]
                            
                            nbhd_sim = neighboring_items %>% 
                              mutate(item_sim = sim[as.character(hour),as.character(hour_j)]) %>% 
                              ungroup() %>% 
                              arrange(desc(item_sim)) 
                            
                            prediction = nbhd_sim %>% top_n(n = K_j, wt = item_sim) %>% summarize(pred = mean(item_sim)) %>% .$pred
                            pred_df[j,"pred"] = prediction
                            
                          }
                          
                          D_pred = bind_rows(D_pred, pred_df)
                        }
                        
                        D_pred
                        
                      }
    
    stopCluster(cl = cl)
    
    
    pred_df = pred_df %>% mutate(test_set = t)
    
    pred_df = left_join(pred_df, 
                        D_feature_hour_test %>% unique() %>% mutate(actual = 1), 
                        by = c("item" = "item", 
                               "hour" = "hour")) %>% mutate(ae = actual - pred)
    
    
    pred_df_t = bind_rows(pred_df_t, pred_df)
    
    
  }
  
  end_inner = Sys.time()
  print(end_inner - start_inner)
  pred_df_total = bind_rows(pred_df_total, pred_df_t)
}
end = Sys.time()
print(end - start)
