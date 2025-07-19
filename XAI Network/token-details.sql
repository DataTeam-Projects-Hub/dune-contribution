select *
from (values
  ('0x4CB9A7AE498CEDCBB5EAE9F25736AE7D428C9D66', 'Xai'),
  ('0x4C749d097832DE2FEcc989ce18fDc5f1BD76700c', 'EsXai')
) as token_list(token_address, token_name)
