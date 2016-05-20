# -*- coding: utf-8 -*-
"""
    .tools
    ~~~~~~~~~~~~~~~~~~
    
    
    :copyright: (c) 2015 Lighthouse
    :license: LGPL
"""
from random import choice
from string import ascii_letters, digits
import collections

__author__ = 'Matt Badger'


def random_string(length=5):
    lis = list(ascii_letters + digits)
    return ''.join(choice(lis) for _ in xrange(length))


def generate_autocomplete_tokens(original_string):
    if not original_string:
        return ''
    suggestions = []
    for word in original_string.split():
        prefix = ""
        for letter in word:
            prefix += letter
            suggestions.append(prefix)
    return ' '.join(suggestions)


def eval_boolean_string(source):
    return True if source in ['true', '1', 't', 'y', 'yes', 'Yes', 'TRUE', 'True', 'T'] else False


def convert_to_unicode(raw_string):
    if isinstance(raw_string, str):
        return raw_string.decode('latin1')
    else:
        return raw_string


def csv_item_convert(data):
    if isinstance(data, basestring):
        return data.encode('utf-8')
    elif isinstance(data, collections.Mapping):
        return dict(map(csv_item_convert, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(csv_item_convert, data))
    else:
        return data


# TODO: convert these to be implementation independant
# def html_actions_table_from_dict(target, link_dict):
#     list_items = []
#
#     if link_dict:
#         for target_key, link_details in link_dict.iteritems():
#             list_items.append(u'<tr><td><a href="{target}?key={target_key}&referrer=directory" target="_blank">{link_name}</a></td><td><form action="{emulate_url}" method="post" target="_blank" style="float:right;"><input name="csrf_token" type="hidden" value="{csrf_token}"><input name="item_key" type="hidden" value="{target_key}"><button type="submit">Emulate</button></form></td></tr>'.format(target=target, target_key=target_key, link_name=link_details['name'], emulate_url=u'{emulate_url}', csrf_token=u'{csrf_token}'))
#
#     compiled = u'\n'.join(list_items) if list_items else u''
#     return u'<table>{0}</table>'.format(compiled)
#
#
# def html_link_list_from_dict(target, link_dict):
#     list_items = []
#
#     if link_dict:
#         for target_key, link_details in link_dict.iteritems():
#             list_items.append(u'<li><a href="{0}?key={1}&referrer=directory" target="_blank">{2}</a></li>'.format(target, target_key, link_details['name']))
#
#     compiled = u'\n'.join(list_items) if list_items else u''
#     return u'<ul>{0}</ul>'.format(compiled)


"""
A dictionary difference calculator
Originally posted as:
http://stackoverflow.com/questions/1165352/fast-comparison-between-two-python-dictionary/1165552#1165552

Original Author:
https://github.com/hughdbrown/dictdiffer
"""


class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.current_keys, self.past_keys = [
            set(d.keys()) for d in (current_dict, past_dict)
        ]
        self.intersect = self.current_keys.intersection(self.past_keys)

    def added(self):
        return self.current_keys - self.intersect

    def removed(self):
        return self.past_keys - self.intersect

    def changed(self):
        return set(o for o in self.intersect
                   if self.past_dict[o] != self.current_dict[o])

    def unchanged(self):
        return set(o for o in self.intersect
                   if self.past_dict[o] == self.current_dict[o])

STATIC_COUNTRY_LABLES_TUPLE = (
    (u'AF', u'Afghanistan'), (u'AX', u'\xc5land Islands'), (u'AL', u'Albania'), (u'DZ', u'Algeria'),
    (u'AS', u'American Samoa'), (u'AD', u'Andorra'), (u'AO', u'Angola'), (u'AI', u'Anguilla'),
    (u'AQ', u'Antarctica'), (u'AG', u'Antigua and Barbuda'), (u'AR', u'Argentina'),
    (u'AM', u'Armenia'), (u'AW', u'Aruba'), (u'AU', u'Australia'), (u'AT', u'Austria'),
    (u'AZ', u'Azerbaijan'), (u'BS', u'Bahamas'), (u'BH', u'Bahrain'), (u'BD', u'Bangladesh'),
    (u'BB', u'Barbados'), (u'BY', u'Belarus'), (u'BE', u'Belgium'), (u'BZ', u'Belize'),
    (u'BJ', u'Benin'), (u'BM', u'Bermuda'), (u'BT', u'Bhutan'),
    (u'BO', u'Bolivia, Plurinational State of'), (u'BQ', u'Bonaire, Sint Eustatius and Saba'),
    (u'BA', u'Bosnia and Herzegovina'), (u'BW', u'Botswana'), (u'BV', u'Bouvet Island'),
    (u'BR', u'Brazil'), (u'IO', u'British Indian Ocean Territory'), (u'BN', u'Brunei Darussalam'),
    (u'BG', u'Bulgaria'), (u'BF', u'Burkina Faso'), (u'BI', u'Burundi'), (u'KH', u'Cambodia'),
    (u'CM', u'Cameroon'), (u'CA', u'Canada'), (u'CV', u'Cape Verde'), (u'KY', u'Cayman Islands'),
    (u'CF', u'Central African Republic'), (u'TD', u'Chad'), (u'CL', u'Chile'), (u'CN', u'China'),
    (u'CX', u'Christmas Island'), (u'CC', u'Cocos (Keeling) Islands'), (u'CO', u'Colombia'),
    (u'KM', u'Comoros'), (u'CG', u'Congo'), (u'CD', u'Congo, The Democratic Republic of the'),
    (u'CK', u'Cook Islands'), (u'CR', u'Costa Rica'), (u'CI', u"C\xf4te d'Ivoire"),
    (u'HR', u'Croatia'), (u'CU', u'Cuba'), (u'CW', u'Cura\xe7ao'), (u'CY', u'Cyprus'),
    (u'CZ', u'Czech Republic'), (u'DK', u'Denmark'), (u'DJ', u'Djibouti'), (u'DM', u'Dominica'),
    (u'DO', u'Dominican Republic'), (u'EC', u'Ecuador'), (u'EG', u'Egypt'), (u'SV', u'El Salvador'),
    (u'GQ', u'Equatorial Guinea'), (u'ER', u'Eritrea'), (u'EE', u'Estonia'), (u'ET', u'Ethiopia'),
    (u'FK', u'Falkland Islands (Malvinas)'), (u'FO', u'Faroe Islands'), (u'FJ', u'Fiji'),
    (u'FI', u'Finland'), (u'FR', u'France'), (u'GF', u'French Guiana'), (u'PF', u'French Polynesia'),
    (u'TF', u'French Southern Territories'), (u'GA', u'Gabon'), (u'GM', u'Gambia'),
    (u'GE', u'Georgia'), (u'DE', u'Germany'), (u'GH', u'Ghana'), (u'GI', u'Gibraltar'),
    (u'GR', u'Greece'), (u'GL', u'Greenland'), (u'GD', u'Grenada'), (u'GP', u'Guadeloupe'),
    (u'GU', u'Guam'), (u'GT', u'Guatemala'), (u'GG', u'Guernsey'), (u'GN', u'Guinea'),
    (u'GW', u'Guinea-Bissau'), (u'GY', u'Guyana'), (u'HT', u'Haiti'),
    (u'HM', u'Heard Island and McDonald Islands'), (u'VA', u'Holy See (Vatican City State)'),
    (u'HN', u'Honduras'), (u'HK', u'Hong Kong'), (u'HU', u'Hungary'), (u'IS', u'Iceland'),
    (u'IN', u'India'), (u'ID', u'Indonesia'), (u'IR', u'Iran, Islamic Republic of'),
    (u'IQ', u'Iraq'), (u'IE', u'Ireland'), (u'IM', u'Isle of Man'), (u'IL', u'Israel'),
    (u'IT', u'Italy'), (u'JM', u'Jamaica'), (u'JP', u'Japan'), (u'JE', u'Jersey'),
    (u'JO', u'Jordan'), (u'KZ', u'Kazakhstan'), (u'KE', u'Kenya'), (u'KI', u'Kiribati'),
    (u'KP', u"Korea, Democratic People's Republic of"), (u'KR', u'Korea, Republic of'),
    (u'KW', u'Kuwait'), (u'KG', u'Kyrgyzstan'), (u'LA', u"Lao People's Democratic Republic"),
    (u'LV', u'Latvia'), (u'LB', u'Lebanon'), (u'LS', u'Lesotho'), (u'LR', u'Liberia'),
    (u'LY', u'Libya'), (u'LI', u'Liechtenstein'), (u'LT', u'Lithuania'), (u'LU', u'Luxembourg'),
    (u'MO', u'Macao'), (u'MK', u'Macedonia, Republic of'), (u'MG', u'Madagascar'),
    (u'MW', u'Malawi'), (u'MY', u'Malaysia'), (u'MV', u'Maldives'), (u'ML', u'Mali'),
    (u'MT', u'Malta'), (u'MH', u'Marshall Islands'), (u'MQ', u'Martinique'), (u'MR', u'Mauritania'),
    (u'MU', u'Mauritius'), (u'YT', u'Mayotte'), (u'MX', u'Mexico'),
    (u'FM', u'Micronesia, Federated States of'), (u'MD', u'Moldova, Republic of'),
    (u'MC', u'Monaco'), (u'MN', u'Mongolia'), (u'ME', u'Montenegro'), (u'MS', u'Montserrat'),
    (u'MA', u'Morocco'), (u'MZ', u'Mozambique'), (u'MM', u'Myanmar'), (u'NA', u'Namibia'),
    (u'NR', u'Nauru'), (u'NP', u'Nepal'), (u'NL', u'Netherlands'), (u'NC', u'New Caledonia'),
    (u'NZ', u'New Zealand'), (u'NI', u'Nicaragua'), (u'NE', u'Niger'), (u'NG', u'Nigeria'),
    (u'NU', u'Niue'), (u'NF', u'Norfolk Island'), (u'MP', u'Northern Mariana Islands'),
    (u'NO', u'Norway'), (u'OM', u'Oman'), (u'PK', u'Pakistan'), (u'PW', u'Palau'),
    (u'PS', u'Palestine, State of'), (u'PA', u'Panama'), (u'PG', u'Papua New Guinea'),
    (u'PY', u'Paraguay'), (u'PE', u'Peru'), (u'PH', u'Philippines'), (u'PN', u'Pitcairn'),
    (u'PL', u'Poland'), (u'PT', u'Portugal'), (u'PR', u'Puerto Rico'), (u'QA', u'Qatar'),
    (u'RE', u'R\xe9union'), (u'RO', u'Romania'), (u'RU', u'Russian Federation'), (u'RW', u'Rwanda'),
    (u'BL', u'Saint Barth\xe9lemy'), (u'SH', u'Saint Helena, Ascension and Tristan da Cunha'),
    (u'KN', u'Saint Kitts and Nevis'), (u'LC', u'Saint Lucia'),
    (u'MF', u'Saint Martin (French part)'), (u'PM', u'Saint Pierre and Miquelon'),
    (u'VC', u'Saint Vincent and the Grenadines'), (u'WS', u'Samoa'), (u'SM', u'San Marino'),
    (u'ST', u'Sao Tome and Principe'), (u'SA', u'Saudi Arabia'), (u'SN', u'Senegal'),
    (u'RS', u'Serbia'), (u'SC', u'Seychelles'), (u'SL', u'Sierra Leone'), (u'SG', u'Singapore'),
    (u'SX', u'Sint Maarten (Dutch part)'), (u'SK', u'Slovakia'), (u'SI', u'Slovenia'),
    (u'SB', u'Solomon Islands'), (u'SO', u'Somalia'), (u'ZA', u'South Africa'),
    (u'GS', u'South Georgia and the South Sandwich Islands'), (u'ES', u'Spain'),
    (u'LK', u'Sri Lanka'), (u'SD', u'Sudan'), (u'SR', u'Suriname'), (u'SS', u'South Sudan'),
    (u'SJ', u'Svalbard and Jan Mayen'), (u'SZ', u'Swaziland'), (u'SE', u'Sweden'),
    (u'CH', u'Switzerland'), (u'SY', u'Syrian Arab Republic'), (u'TW', u'Taiwan, Province of China'),
    (u'TJ', u'Tajikistan'), (u'TZ', u'Tanzania, United Republic of'), (u'TH', u'Thailand'),
    (u'TL', u'Timor-Leste'), (u'TG', u'Togo'), (u'TK', u'Tokelau'), (u'TO', u'Tonga'),
    (u'TT', u'Trinidad and Tobago'), (u'TN', u'Tunisia'), (u'TR', u'Turkey'),
    (u'TM', u'Turkmenistan'), (u'TC', u'Turks and Caicos Islands'), (u'TV', u'Tuvalu'),
    (u'UG', u'Uganda'), (u'UA', u'Ukraine'), (u'AE', u'United Arab Emirates'),
    (u'GB', u'United Kingdom'), (u'US', u'United States'),
    (u'UM', u'United States Minor Outlying Islands'), (u'UY', u'Uruguay'), (u'UZ', u'Uzbekistan'),
    (u'VU', u'Vanuatu'), (u'VE', u'Venezuela, Bolivarian Republic of'), (u'VN', u'Viet Nam'),
    (u'VG', u'Virgin Islands, British'), (u'VI', u'Virgin Islands, U.S.'),
    (u'WF', u'Wallis and Futuna'), (u'EH', u'Western Sahara'), (u'YE', u'Yemen'), (u'ZM', u'Zambia'),
    (u'ZW', u'Zimbabwe')
)

STATIC_COUNTRY_CODES_TUPLE = (
    u'YE', u'LK', u'LI', u'DZ', u'LC', u'LA', u'DE', u'SN', u'YT', u'LY', u'LV', u'DO', u'LT', u'DM', u'DJ', u'DK',
    u'TF', u'TG', u'TD', u'TC', u'TN', u'TO', u'TL', u'TM', u'TJ', u'TK', u'TH', u'TV', u'TW', u'TT', u'TR', u'TZ',
    u'VU', u'GY', u'GW', u'LB', u'GU', u'GT', u'GS', u'GR', u'GQ', u'GP', u'GN', u'GM', u'GL', u'GI', u'GH', u'GG',
    u'GF', u'GE', u'GD', u'GB', u'GA', u'WF', u'ZM', u'OM', u'WS', u'BD', u'BE', u'BF', u'BG', u'BA', u'BB', u'ML',
    u'BL', u'BM', u'BN', u'BO', u'BH', u'BI', u'BJ', u'BT', u'BV', u'BW', u'BQ', u'BR', u'BS', u'BY', u'BZ', u'LU',
    u'ES', u'LR', u'RS', u'JP', u'LS', u'JM', u'JO', u'JE', u'MM', u'ET', u'MO', u'MN', u'MH', u'MK', u'ER', u'ME',
    u'MD', u'MG', u'MF', u'MA', u'ZA', u'MC', u'EE', u'RE', u'EG', u'MY', u'MX', u'EC', u'MZ', u'MU', u'MT', u'MW',
    u'MV', u'MQ', u'MP', u'MS', u'MR', u'SJ', u'UG', u'UA', u'PL', u'UM', u'PM', u'US', u'RU', u'UY', u'UZ', u'SR',
    u'ZW', u'HR', u'PK', u'PH', u'RO', u'PN', u'HT', u'HU', u'PA', u'PF', u'PG', u'PE', u'PY', u'SZ', u'PR', u'HK',
    u'HN', u'PW', u'PT', u'HM', u'CC', u'CA', u'SX', u'CG', u'CF', u'CD', u'CK', u'CI', u'CH', u'CO', u'CN', u'CM',
    u'CL', u'CR', u'EH', u'CW', u'CV', u'CU', u'PS', u'CZ', u'CY', u'CX', u'KZ', u'KY', u'SB', u'KR', u'KP', u'KW',
    u'SA', u'KI', u'KH', u'KN', u'KM', u'KG', u'KE', u'SS', u'NI', u'FR', u'NL', u'SV', u'NO', u'NA', u'SY', u'NC',
    u'NE', u'NF', u'NG', u'SC', u'SK', u'NZ', u'SG', u'SE', u'SD', u'NP', u'FI', u'FJ', u'FK', u'SO', u'FM', u'SM',
    u'FO', u'VA', u'SI', u'SH', u'VE', u'VG', u'VI', u'VN', u'NU', u'NR', u'SL', u'AX', u'AZ', u'ST', u'AQ', u'AS',
    u'AR', u'AU', u'AT', u'AW', u'AI', u'VC', u'AM', u'AL', u'AO', u'AE', u'AD', u'AG', u'AF', u'IQ', u'IS', u'IR',
    u'IT', u'QA', u'RW', u'IE', u'ID', u'IM', u'IL', u'IO', u'IN'
)

STATIC_COUNTRY_CODES_SET = {
    u'YE', u'LK', u'LI', u'DZ', u'LC', u'LA', u'DE', u'SN', u'YT', u'LY', u'LV', u'DO', u'LT', u'DM', u'DJ', u'DK',
    u'TF', u'TG', u'TD', u'TC', u'TN', u'TO', u'TL', u'TM', u'TJ', u'TK', u'TH', u'TV', u'TW', u'TT', u'TR', u'TZ',
    u'VU', u'GY', u'GW', u'LB', u'GU', u'GT', u'GS', u'GR', u'GQ', u'GP', u'GN', u'GM', u'GL', u'GI', u'GH', u'GG',
    u'GF', u'GE', u'GD', u'GB', u'GA', u'WF', u'ZM', u'OM', u'WS', u'BD', u'BE', u'BF', u'BG', u'BA', u'BB', u'ML',
    u'BL', u'BM', u'BN', u'BO', u'BH', u'BI', u'BJ', u'BT', u'BV', u'BW', u'BQ', u'BR', u'BS', u'BY', u'BZ', u'LU',
    u'ES', u'LR', u'RS', u'JP', u'LS', u'JM', u'JO', u'JE', u'MM', u'ET', u'MO', u'MN', u'MH', u'MK', u'ER', u'ME',
    u'MD', u'MG', u'MF', u'MA', u'ZA', u'MC', u'EE', u'RE', u'EG', u'MY', u'MX', u'EC', u'MZ', u'MU', u'MT', u'MW',
    u'MV', u'MQ', u'MP', u'MS', u'MR', u'SJ', u'UG', u'UA', u'PL', u'UM', u'PM', u'US', u'RU', u'UY', u'UZ', u'SR',
    u'ZW', u'HR', u'PK', u'PH', u'RO', u'PN', u'HT', u'HU', u'PA', u'PF', u'PG', u'PE', u'PY', u'SZ', u'PR', u'HK',
    u'HN', u'PW', u'PT', u'HM', u'CC', u'CA', u'SX', u'CG', u'CF', u'CD', u'CK', u'CI', u'CH', u'CO', u'CN', u'CM',
    u'CL', u'CR', u'EH', u'CW', u'CV', u'CU', u'PS', u'CZ', u'CY', u'CX', u'KZ', u'KY', u'SB', u'KR', u'KP', u'KW',
    u'SA', u'KI', u'KH', u'KN', u'KM', u'KG', u'KE', u'SS', u'NI', u'FR', u'NL', u'SV', u'NO', u'NA', u'SY', u'NC',
    u'NE', u'NF', u'NG', u'SC', u'SK', u'NZ', u'SG', u'SE', u'SD', u'NP', u'FI', u'FJ', u'FK', u'SO', u'FM', u'SM',
    u'FO', u'VA', u'SI', u'SH', u'VE', u'VG', u'VI', u'VN', u'NU', u'NR', u'SL', u'AX', u'AZ', u'ST', u'AQ', u'AS',
    u'AR', u'AU', u'AT', u'AW', u'AI', u'VC', u'AM', u'AL', u'AO', u'AE', u'AD', u'AG', u'AF', u'IQ', u'IS', u'IR',
    u'IT', u'QA', u'RW', u'IE', u'ID', u'IM', u'IL', u'IO', u'IN'
}


STATIC_LANGUAGE_CODES_TUPLE = (
    (u'aa', u'Afar'),
    (u'ab', u'Abkhazian'),
    (u'af', u'Afrikaans'),
    (u'ak', u'Akan'),
    (u'sq', u'Albanian'),
    (u'am', u'Amharic'),
    (u'ar', u'Arabic'),
    (u'an', u'Aragonese'),
    (u'hy', u'Armenian'),
    (u'as', u'Assamese'),
    (u'av', u'Avaric'),
    (u'ae', u'Avestan'),
    (u'ay', u'Aymara'),
    (u'az', u'Azerbaijani'),
    (u'ba', u'Bashkir'),
    (u'bm', u'Bambara'),
    (u'eu', u'Basque'),
    (u'be', u'Belarusian'),
    (u'bn', u'Bengali'),
    (u'bh', u'Bihari languages'),
    (u'bi', u'Bislama'),
    (u'bo', u'Tibetan'),
    (u'bs', u'Bosnian'),
    (u'br', u'Breton'),
    (u'bg', u'Bulgarian'),
    (u'my', u'Burmese'),
    (u'ca', u'Catalan; Valencian'),
    (u'cs', u'Czech'),
    (u'ch', u'Chamorro'),
    (u'ce', u'Chechen'),
    (u'zh', u'Chinese'),
    (u'cu', u'Church Slavic; Old Slavonic; Church Slavonic; Old Bulgarian; Old Church Slavonic'),
    (u'cv', u'Chuvash'),
    (u'kw', u'Cornish'),
    (u'co', u'Corsican'),
    (u'cr', u'Cree'),
    (u'cy', u'Welsh'),
    (u'cs', u'Czech'),
    (u'da', u'Danish'),
    (u'de', u'German'),
    (u'dv', u'Divehi; Dhivehi; Maldivian'),
    (u'nl', u'Dutch; Flemish'),
    (u'dz', u'Dzongkha'),
    (u'el', u'Greek, Modern (1453-)'),
    (u'en', u'English'),
    (u'eo', u'Esperanto'),
    (u'et', u'Estonian'),
    (u'eu', u'Basque'),
    (u'ee', u'Ewe'),
    (u'fo', u'Faroese'),
    (u'fa', u'Persian'),
    (u'fj', u'Fijian'),
    (u'fi', u'Finnish'),
    (u'fr', u'French'),
    (u'fr', u'French'),
    (u'fy', u'Western Frisian'),
    (u'ff', u'Fulah'),
    (u'Ga', u'Georgian'),
    (u'de', u'German'),
    (u'gd', u'Gaelic; Scottish Gaelic'),
    (u'ga', u'Irish'),
    (u'gl', u'Galician'),
    (u'gv', u'Manx'),
    (u'el', u'Greek, Modern (1453-)'),
    (u'gn', u'Guarani'),
    (u'gu', u'Gujarati'),
    (u'ht', u'Haitian; Haitian Creole'),
    (u'ha', u'Hausa'),
    (u'he', u'Hebrew'),
    (u'hz', u'Herero'),
    (u'hi', u'Hindi'),
    (u'ho', u'Hiri Motu'),
    (u'hr', u'Croatian'),
    (u'hu', u'Hungarian'),
    (u'hy', u'Armenian'),
    (u'ig', u'Igbo'),
    (u'is', u'Icelandic'),
    (u'io', u'Ido'),
    (u'ii', u'Sichuan Yi; Nuosu'),
    (u'iu', u'Inuktitut'),
    (u'ie', u'Interlingue; Occidental'),
    (u'ia', u'Interlingua (International Auxiliary Language Association)'),
    (u'id', u'Indonesian'),
    (u'ik', u'Inupiaq'),
    (u'is', u'Icelandic'),
    (u'it', u'Italian'),
    (u'jv', u'Javanese'),
    (u'ja', u'Japanese'),
    (u'kl', u'Kalaallisut; Greenlandic'),
    (u'kn', u'Kannada'),
    (u'ks', u'Kashmiri'),
    (u'ka', u'Georgian'),
    (u'kr', u'Kanuri'),
    (u'kk', u'Kazakh'),
    (u'km', u'Central Khmer'),
    (u'ki', u'Kikuyu; Gikuyu'),
    (u'rw', u'Kinyarwanda'),
    (u'ky', u'Kirghiz; Kyrgyz'),
    (u'kv', u'Komi'),
    (u'kg', u'Kongo'),
    (u'ko', u'Korean'),
    (u'kj', u'Kuanyama; Kwanyama'),
    (u'ku', u'Kurdish'),
    (u'lo', u'Lao'),
    (u'la', u'Latin'),
    (u'lv', u'Latvian'),
    (u'li', u'Limburgan; Limburger; Limburgish'),
    (u'ln', u'Lingala'),
    (u'lt', u'Lithuanian'),
    (u'lb', u'Luxembourgish; Letzeburgesch'),
    (u'lu', u'Luba-Katanga'),
    (u'lg', u'Ganda'),
    (u'mk', u'Macedonian'),
    (u'mh', u'Marshallese'),
    (u'ml', u'Malayalam'),
    (u'mi', u'Maori'),
    (u'mr', u'Marathi'),
    (u'ms', u'Malay'),
    (u'Mi', u'Micmac'),
    (u'mk', u'Macedonian'),
    (u'mg', u'Malagasy'),
    (u'mt', u'Maltese'),
    (u'mn', u'Mongolian'),
    (u'mi', u'Maori'),
    (u'ms', u'Malay'),
    (u'my', u'Burmese'),
    (u'na', u'Nauru'),
    (u'nv', u'Navajo; Navaho'),
    (u'nr', u'Ndebele, South; South Ndebele'),
    (u'nd', u'Ndebele, North; North Ndebele'),
    (u'ng', u'Ndonga'),
    (u'ne', u'Nepali'),
    (u'nl', u'Dutch; Flemish'),
    (u'nn', u'Norwegian Nynorsk; Nynorsk, Norwegian'),
    (u'nb', u'BokmÃ¥l, Norwegian; Norwegian BokmÃ¥l'),
    (u'no', u'Norwegian'),
    (u'oc', u'Occitan (post 1500)'),
    (u'oj', u'Ojibwa'),
    (u'or', u'Oriya'),
    (u'om', u'Oromo'),
    (u'os', u'Ossetian; Ossetic'),
    (u'pa', u'Panjabi; Punjabi'),
    (u'fa', u'Persian'),
    (u'pi', u'Pali'),
    (u'pl', u'Polish'),
    (u'pt', u'Portuguese'),
    (u'ps', u'Pushto; Pashto'),
    (u'qu', u'Quechua'),
    (u'rm', u'Romansh'),
    (u'ro', u'Romanian; Moldavian; Moldovan'),
    (u'ro', u'Romanian; Moldavian; Moldovan'),
    (u'rn', u'Rundi'),
    (u'ru', u'Russian'),
    (u'sg', u'Sango'),
    (u'sa', u'Sanskrit'),
    (u'si', u'Sinhala; Sinhalese'),
    (u'sk', u'Slovak'),
    (u'sk', u'Slovak'),
    (u'sl', u'Slovenian'),
    (u'se', u'Northern Sami'),
    (u'sm', u'Samoan'),
    (u'sn', u'Shona'),
    (u'sd', u'Sindhi'),
    (u'so', u'Somali'),
    (u'st', u'Sotho, Southern'),
    (u'es', u'Spanish; Castilian'),
    (u'sq', u'Albanian'),
    (u'sc', u'Sardinian'),
    (u'sr', u'Serbian'),
    (u'ss', u'Swati'),
    (u'su', u'Sundanese'),
    (u'sw', u'Swahili'),
    (u'sv', u'Swedish'),
    (u'ty', u'Tahitian'),
    (u'ta', u'Tamil'),
    (u'tt', u'Tatar'),
    (u'te', u'Telugu'),
    (u'tg', u'Tajik'),
    (u'tl', u'Tagalog'),
    (u'th', u'Thai'),
    (u'bo', u'Tibetan'),
    (u'ti', u'Tigrinya'),
    (u'to', u'Tonga (Tonga Islands)'),
    (u'tn', u'Tswana'),
    (u'ts', u'Tsonga'),
    (u'tk', u'Turkmen'),
    (u'tr', u'Turkish'),
    (u'tw', u'Twi'),
    (u'ug', u'Uighur; Uyghur'),
    (u'uk', u'Ukrainian'),
    (u'ur', u'Urdu'),
    (u'uz', u'Uzbek'),
    (u've', u'Venda'),
    (u'vi', u'Vietnamese'),
    (u'vo', u'VolapÃ¼k'),
    (u'cy', u'Welsh'),
    (u'wa', u'Walloon'),
    (u'wo', u'Wolof'),
    (u'xh', u'Xhosa'),
    (u'yi', u'Yiddish'),
    (u'yo', u'Yoruba'),
    (u'za', u'Zhuang; Chuang'),
    (u'zh', u'Chinese'),
    (u'zu', u'Zulu')
)

STATIC_LANGUAGE_CODES_SET = {
    u'aa',
    u'ab',
    u'af',
    u'ak',
    u'sq',
    u'am',
    u'ar',
    u'an',
    u'hy',
    u'as',
    u'av',
    u'ae',
    u'ay',
    u'az',
    u'ba',
    u'bm',
    u'eu',
    u'be',
    u'bn',
    u'bh',
    u'bi',
    u'bo',
    u'bs',
    u'br',
    u'bg',
    u'my',
    u'ca',
    u'cs',
    u'ch',
    u'ce',
    u'zh',
    u'cu',
    u'cv',
    u'kw',
    u'co',
    u'cr',
    u'cy',
    u'cs',
    u'da',
    u'de',
    u'dv',
    u'nl',
    u'dz',
    u'el',
    u'en',
    u'eo',
    u'et',
    u'eu',
    u'ee',
    u'fo',
    u'fa',
    u'fj',
    u'fi',
    u'fr',
    u'fr',
    u'fy',
    u'ff',
    u'Ga',
    u'de',
    u'gd',
    u'ga',
    u'gl',
    u'gv',
    u'el',
    u'gn',
    u'gu',
    u'ht',
    u'ha',
    u'he',
    u'hz',
    u'hi',
    u'ho',
    u'hr',
    u'hu',
    u'hy',
    u'ig',
    u'is',
    u'io',
    u'ii',
    u'iu',
    u'ie',
    u'ia',
    u'id',
    u'ik',
    u'is',
    u'it',
    u'jv',
    u'ja',
    u'kl',
    u'kn',
    u'ks',
    u'ka',
    u'kr',
    u'kk',
    u'km',
    u'ki',
    u'rw',
    u'ky',
    u'kv',
    u'kg',
    u'ko',
    u'kj',
    u'ku',
    u'lo',
    u'la',
    u'lv',
    u'li',
    u'ln',
    u'lt',
    u'lb',
    u'lu',
    u'lg',
    u'mk',
    u'mh',
    u'ml',
    u'mi',
    u'mr',
    u'ms',
    u'Mi',
    u'mk',
    u'mg',
    u'mt',
    u'mn',
    u'mi',
    u'ms',
    u'my',
    u'na',
    u'nv',
    u'nr',
    u'nd',
    u'ng',
    u'ne',
    u'nl',
    u'nn',
    u'nb',
    u'no',
    u'oc',
    u'oj',
    u'or',
    u'om',
    u'os',
    u'pa',
    u'fa',
    u'pi',
    u'pl',
    u'pt',
    u'ps',
    u'qu',
    u'rm',
    u'ro',
    u'ro',
    u'rn',
    u'ru',
    u'sg',
    u'sa',
    u'si',
    u'sk',
    u'sk',
    u'sl',
    u'se',
    u'sm',
    u'sn',
    u'sd',
    u'so',
    u'st',
    u'es',
    u'sq',
    u'sc',
    u'sr',
    u'ss',
    u'su',
    u'sw',
    u'sv',
    u'ty',
    u'ta',
    u'tt',
    u'te',
    u'tg',
    u'tl',
    u'th',
    u'bo',
    u'ti',
    u'to',
    u'tn',
    u'ts',
    u'tk',
    u'tr',
    u'tw',
    u'ug',
    u'uk',
    u'ur',
    u'uz',
    u've',
    u'vi',
    u'vo',
    u'cy',
    u'wa',
    u'wo',
    u'xh',
    u'yi',
    u'yo',
    u'za',
    u'zh',
    u'zu',
}


def eu_country(country_code):
    eu_list = {'BE',
               'BG',
               'CZ',
               'DK',
               'DE',
               'EE',
               'IE',
               'GR',
               'ES',
               'FR',
               'HR',
               'IT',
               'CY',
               'LV',
               'LT',
               'LU',
               'HU',
               'MT',
               'NL',
               'AT',
               'PL',
               'PT',
               'RO',
               'SI',
               'SK',
               'FI',
               'SE',
               'GB'}
    check = {country_code}
    if len(eu_list - check) < 28:
        return True
    else:
        return False
