# jsmrexercises

Sample MR doing secondary sort. 

1. Fitering: 
   - package exerciseJS corresponds to the filter problem. 
   - filterMR is the driver class. 
   - The mapper class: FilterMapper creates a composite key (TextP) of user and time. TextP implements the compareTo 
      function to help sort records by user and then time.
   - The job uses a Partitioner: FilterPartitioner that partions only by user.
   - The job uses a groupingcomparator; FilterGroupComparator that groups by the user.  
   - The reducer class: FilterReducer reads the list of patterns from a file, and then for each pattern,
      it goes over the sorted (by timestamp) list of urls, and created the final output of ts,user,url,prevurl
 
 Caveat: none

2. Statistics.
   - package exerciseJS2 corresponds to the statistics problem. 
   - DomainStatMR is the driver class. 
   - The mapper class: DomainStatMapper creates a composite key (TextTriplet) of domain, user and time. TextTriplet
      implements the compareTo function to help sort records by domain, user and then time.
   - The job uses a Partitioner: DomainStatPartitioner that partions only by domain.
   - The job uses a groupingcomparator; DomainStatGroupComparator that groups by the domain.  
   - The reducer class: For each domain, DomainStatReducer goes over the sorted (by user andtimestamp) list of (user, ts), and created the final output of domain 'totalviews,numberofvisitors,totalvisits'
   
   Caveat: One issue I see is the possibility of a massive skew in some cases where a few domains have 10s of billions of values. That might end up being seen as outofmemory problem in the reducer(s). To solve this, perhaps one needs to do some sampling of the input space. Let us assume we see 10 billion clicks. In the sample, if we see any site that comes up mire than 5%, we ask the partitioner to partition that site, not just by domain, but domain and hour.
