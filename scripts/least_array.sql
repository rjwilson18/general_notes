/*
@author Richard Wilson
@date 01/14/22

Returns the smallest element in an array.
This is useful in place of the least function which can not be used if any elements in the list are null
*/
create or replace function 
 `gcp-ent-auto-ds-dev.udf_library.least_array` (arr any type)
 as ((
  select min(element) from unnest(arr) element where element is not null
 ));
