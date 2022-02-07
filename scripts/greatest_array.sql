/*
@author Richard Wilson
@date 01/13/22

Returns the largest element in an array.
This is useful in place of the greater function which can not be used if any elements in the list are null
*/
create or replace function 
 `gcp-ent-auto-ds-dev.udf_library.greatest_array` (arr any type)
 as ((
  select max(element) from unnest(arr) element where element is not null
 ));

