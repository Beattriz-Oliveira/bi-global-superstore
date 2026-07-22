"""
Dicionários de tradução usados pelo ETL (etl_dataset.py).

Separado do pipeline principal para facilitar manutenção: qualquer ajuste em
nomes de coluna ou em valores traduzidos (ex.: adicionar um país que faltou)
é feito aqui, sem mexer na lógica de transformação/carga.
"""

# Nome das colunas
DICIONARIO_COLUNAS = {
    'Category': 'Categoria',
    'City': 'Cidade',
    'Country': 'Pais',
    'Customer ID': 'ID_Cliente',
    'Customer Name': 'Nome_Cliente',
    'Discount': 'Desconto',
    'Market': 'Mercado',
    'Order Date': 'Data_Pedido',
    'Order ID': 'ID_Pedido',
    'Order Priority': 'Prioridade_Pedido',
    'Product ID': 'ID_Produto',
    'Product Name': 'Nome_Produto',
    'Profit': 'Lucro_USD',
    'Quantity': 'Quantidade',
    'Region': 'Regiao',
    'Row ID': 'ID_Linha',
    'Sales': 'Vendas_USD',
    'Segment': 'Segmento',
    'Ship Date': 'Data_Envio',
    'Ship Mode': 'Modo_Envio',
    'Shipping Cost': 'Custo_Envio',
    'State': 'Estado',
    'Sub-Category': 'Sub_Categoria',
    'Year': 'Ano',
    'Market2': 'Mercado2',
    'weeknum': 'N_Semana'
}

# Nome dos paises
_PAISES = {
    'Afghanistan': 'Afeganistão', 'Albania': 'Albânia', 'Algeria': 'Argélia',
    'Angola': 'Angola', 'Argentina': 'Argentina', 'Armenia': 'Armênia',
    'Australia': 'Austrália', 'Austria': 'Áustria', 'Azerbaijan': 'Azerbaijão',
    'Bahrain': 'Barein', 'Bangladesh': 'Bangladesh', 'Barbados': 'Barbados',
    'Belarus': 'Bielorrússia', 'Belgium': 'Bélgica', 'Benin': 'Benin',
    'Bolivia': 'Bolívia', 'Bosnia and Herzegovina': 'Bósnia e Herzegovina',
    'Brazil': 'Brasil', 'Bulgaria': 'Bulgária', 'Burundi': 'Burundi',
    'Cambodia': 'Camboja', 'Cameroon': 'Camarões', 'Canada': 'Canadá',
    'Central African Republic': 'República Centro-Africana', 'Chad': 'Chade',
    'Chile': 'Chile', 'China': 'China', 'Colombia': 'Colômbia',
    "Cote d'Ivoire": 'Costa do Marfim', 'Croatia': 'Croácia', 'Cuba': 'Cuba',
    'Czech Republic': 'República Tcheca',
    'Democratic Republic of the Congo': 'República Democrática do Congo',
    'Denmark': 'Dinamarca', 'Djibouti': 'Djibuti',
    'Dominican Republic': 'República Dominicana', 'Ecuador': 'Equador',
    'Egypt': 'Egito', 'El Salvador': 'El Salvador',
    'Equatorial Guinea': 'Guiné Equatorial', 'Eritrea': 'Eritreia',
    'Estonia': 'Estônia', 'Ethiopia': 'Etiópia', 'Finland': 'Finlândia',
    'France': 'França', 'Gabon': 'Gabão', 'Georgia': 'Geórgia',
    'Germany': 'Alemanha', 'Ghana': 'Gana', 'Guadeloupe': 'Guadalupe',
    'Guatemala': 'Guatemala', 'Guinea': 'Guiné', 'Guinea-Bissau': 'Guiné-Bissau',
    'Haiti': 'Haiti', 'Honduras': 'Honduras', 'Hong Kong': 'Hong Kong',
    'Hungary': 'Hungria', 'India': 'Índia', 'Indonesia': 'Indonésia',
    'Iran': 'Irã', 'Iraq': 'Iraque', 'Ireland': 'Irlanda', 'Israel': 'Israel',
    'Italy': 'Itália', 'Jamaica': 'Jamaica', 'Japan': 'Japão',
    'Jordan': 'Jordânia', 'Kazakhstan': 'Cazaquistão', 'Kenya': 'Quênia',
    'Kyrgyzstan': 'Quirguistão', 'Lebanon': 'Líbano', 'Lesotho': 'Lesoto',
    'Liberia': 'Libéria', 'Libya': 'Líbia', 'Lithuania': 'Lituânia',
    'Macedonia': 'Macedônia do Norte', 'Madagascar': 'Madagascar',
    'Malaysia': 'Malásia', 'Mali': 'Mali', 'Martinique': 'Martinica',
    'Mauritania': 'Mauritânia', 'Mexico': 'México', 'Moldova': 'Moldávia',
    'Mongolia': 'Mongólia', 'Montenegro': 'Montenegro', 'Morocco': 'Marrocos',
    'Mozambique': 'Moçambique', 'Myanmar (Burma)': 'Mianmar',
    'Namibia': 'Namíbia', 'Nepal': 'Nepal', 'Netherlands': 'Países Baixos',
    'New Zealand': 'Nova Zelândia', 'Nicaragua': 'Nicarágua',
    'Niger': 'Níger', 'Nigeria': 'Nigéria', 'Norway': 'Noruega',
    'Pakistan': 'Paquistão', 'Panama': 'Panamá',
    'Papua New Guinea': 'Papua-Nova Guiné', 'Paraguay': 'Paraguai',
    'Peru': 'Peru', 'Philippines': 'Filipinas', 'Poland': 'Polônia',
    'Portugal': 'Portugal', 'Qatar': 'Catar',
    'Republic of the Congo': 'República do Congo', 'Romania': 'Romênia',
    'Russia': 'Rússia', 'Rwanda': 'Ruanda', 'Saudi Arabia': 'Arábia Saudita',
    'Senegal': 'Senegal', 'Sierra Leone': 'Serra Leoa', 'Singapore': 'Singapura',
    'Slovakia': 'Eslováquia', 'Slovenia': 'Eslovênia', 'Somalia': 'Somália',
    'South Africa': 'África do Sul', 'South Korea': 'Coreia do Sul',
    'South Sudan': 'Sudão do Sul', 'Spain': 'Espanha', 'Sri Lanka': 'Sri Lanka',
    'Sudan': 'Sudão', 'Swaziland': 'Essuatíni', 'Sweden': 'Suécia',
    'Switzerland': 'Suíça', 'Syria': 'Síria', 'Taiwan': 'Taiwan',
    'Tajikistan': 'Tadjiquistão', 'Tanzania': 'Tanzânia', 'Thailand': 'Tailândia',
    'Togo': 'Togo', 'Trinidad and Tobago': 'Trinidad e Tobago',
    'Tunisia': 'Tunísia', 'Turkey': 'Turquia', 'Turkmenistan': 'Turcomenistão',
    'Uganda': 'Uganda', 'Ukraine': 'Ucrânia',
    'United Arab Emirates': 'Emirados Árabes Unidos', 'United Kingdom': 'Reino Unido',
    'United States': 'Estados Unidos', 'Uruguay': 'Uruguai',
    'Uzbekistan': 'Uzbequistão', 'Venezuela': 'Venezuela', 'Vietnam': 'Vietnã',
    'Yemen': 'Iêmen', 'Zambia': 'Zâmbia', 'Zimbabwe': 'Zimbábue',
}

# Categora e Subcategoria
MAPEAMENTO_CONTEUDO = {
    'Categoria': {
        'Office Supplies': 'Material de Escritório',
        'Furniture': 'Móveis',
        'Technology': 'Tecnologia'
    },
    'Sub_Categoria': {
        'Paper': 'Papel', 'Binders': 'Fichários', 'Art': 'Arte',
        'Envelopes': 'Envelopes', 'Fasteners': 'Fixadores', 'Labels': 'Etiquetas',
        'Storage': 'Armazenamento', 'Supplies': 'Suprimentos',
        'Appliances': 'Eletrodomésticos', 'Chairs': 'Cadeiras', 'Tables': 'Mesas',
        'Bookcases': 'Estantes', 'Phones': 'Telefones', 'Accessories': 'Acessórios',
        'Copiers': 'Copiadoras', 'Machines': 'Máquinas'
    },
    'Prioridade_Pedido': {
        'High': 'Alta', 'Medium': 'Média', 'Low': 'Baixa', 'Critical': 'Crítica'
    },
    'Modo_Envio': {
        'Standard Class': 'Classe Econômica', 'Second Class': 'Segunda Classe',
        'First Class': 'Primeira Classe', 'Same Day': 'Mesmo Dia'
    },
    'Segmento': {
        'Consumer': 'Consumidor', 'Corporate': 'Corporativo', 'Home Office': 'Home Office'
    },
    'Mercado': {
        'US': 'EUA', 'EU': 'Europa', 'APAC': 'Ásia-Pacífico',
        'LATAM': 'América Latina', 'Africa': 'África', 'EMEA': 'Oriente Médio/África'
    },
    'Mercado2': {
        'North America': 'América do Norte', 'Oceania': 'Oceania',
        'Central America': 'América Central', 'South America': 'América do Sul',
        'Europe': 'Europa', 'Asia': 'Ásia', 'Africa': 'África'
    },
    'Regiao': {
        'West': 'Oeste', 'East': 'Leste', 'Central': 'Central', 'South': 'Sul',
        'North': 'Norte', 'Caribbean': 'Caribe', 'Southeast Asia': 'Sudeste Asiático',
        'Oceania': 'Oceania', 'Central Asia': 'Ásia Central', 'North Asia': 'Norte da Ásia'
    },
    'Pais': _PAISES,
}