# Author: Samantha Piekos
# Date: 3/17/22


# load environment
import pandas as pd 
import numpy as np


# define functions
def generate_fetal_growth_charts():
  import pandas as pd 
  # Define 2015 WHO Fetal Growth Charts (Female, Male, Unknown)
  list_gestational_age = [14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]

  list_female_fetal_growth_percentile_5 = [73, 92, 116, 145, 180, 221, 269, 324, 388, 461, 542, 634, 735, 846, 967, 1096, 1234, 1379, 1530, 1687, 1847, 2008, 2169, 2329, 2484, 2633, 2775]
  list_female_fetal_growth_percentile_10 = [77, 97, 122, 152, 188, 231, 281, 339, 405, 481, 567, 663, 769, 886, 1013, 1150, 1296, 1451, 1614, 1783, 1957, 2135, 2314, 2493, 2670, 2843, 3010]
  list_female_fetal_growth_percentile_25 = [82, 104, 131, 164, 202, 248, 302, 364, 435, 516, 608, 710, 823, 948, 1083, 1230, 1386, 1553, 1728, 1911, 2101, 2296, 2494, 2695, 2896, 3096, 3294]
  list_female_fetal_growth_percentile_50 = [89, 113, 141, 176, 217, 266, 322, 388, 464, 551, 649, 758, 880, 1014, 1160, 1319, 1489, 1670, 1861, 2060, 2268, 2481, 2698, 2917, 3136, 3354, 3567]
  list_female_fetal_growth_percentile_75 = [96, 121, 152, 189, 233, 285, 346, 417, 499, 592, 697, 815, 946, 1090, 1247, 1418, 1601, 1796, 2002, 2217, 2440, 2669, 2902, 3138, 3373, 3605, 3832]
  list_female_fetal_growth_percentile_90 = [102, 129, 162, 202, 248, 304, 369, 444, 530, 629, 740, 865, 1003, 1156, 1323, 1505, 1699, 1907, 2127, 2358, 2598, 2846, 3099, 3357, 3616, 3875, 4131]
  list_female_fetal_growth_percentile_95 = [107, 135, 170, 211, 261, 319, 387, 466, 557, 660, 776, 907, 1051, 1210, 1383, 1570, 1770, 1984, 2209, 2445, 2690, 2943, 3201, 3462, 3725, 3988, 4247]

  list_male_fetal_growth_percentile_5 = [75, 96, 121, 152, 188, 232, 282, 341, 408, 484, 570, 666, 772, 888, 1014, 1149, 1293, 1445, 1605, 1770, 1941, 2114, 2290, 2466, 2641, 2813, 2981]
  list_male_fetal_growth_percentile_10 = [79, 100, 127, 158, 196, 241, 293, 354, 424, 503, 592, 692, 803, 924, 1055, 1197, 1349, 1508, 1677, 1852, 2032, 2217, 2404, 2591, 2778, 2962, 3142]
  list_male_fetal_growth_percentile_25 = [84, 107, 136, 170, 210, 258, 314, 380, 454, 539, 635, 742, 860, 989, 1129, 1281, 1442, 1613, 1793, 1980, 2174, 2372, 2574, 2777, 2981, 3183, 3382]
  list_male_fetal_growth_percentile_50 = [92, 116, 146, 183, 226, 277, 337, 407, 487, 578, 681, 795, 923, 1063, 1215, 1379, 1555, 1741, 1937, 2140, 2350, 2565, 2783, 3001, 3218, 3432, 3639]
  list_male_fetal_growth_percentile_75 = [99, 126, 158, 197, 243, 298, 362, 436, 522, 619, 730, 853, 990, 1141, 1305, 1482, 1672, 1874, 2085, 2306, 2534, 2767, 3002, 3238, 3472, 3701, 3923]
  list_male_fetal_growth_percentile_90 = [105, 134, 169, 210, 260, 320, 389, 469, 561, 666, 785, 917, 1063, 1224, 1399, 1587, 1788, 2000, 2224, 2456, 2694, 2938, 3185, 3432, 3676, 3916, 4149]
  list_male_fetal_growth_percentile_95 = [109,139, 175, 219, 271, 333, 405, 489, 586, 695, 818, 956, 1109, 1276, 1458, 1654, 1863, 2085, 2319, 2562, 2814, 3072, 3334, 3598, 3863, 4125, 4383]

  list_unknown_fetal_growth_percentile_2_point_5 = [70, 89, 113, 141, 174, 214, 260, 314, 375, 445, 523, 611, 707, 813, 929, 1053, 1185, 1326, 1473, 1626, 1785, 1948, 2113, 2280, 2446, 2612, 2775]
  list_unknown_fetal_growth_percentile_5 = [73, 93, 117, 146, 181, 223, 271, 327, 32, 465, 548, 641, 743, 855, 977, 1108, 1247, 1394, 1548, 1708, 1872, 2038, 2205, 2372, 2536, 2696, 2849]
  list_unknown_fetal_growth_percentile_10 = [78, 99, 124, 155, 192, 235, 286, 345, 412, 489, 576, 673, 780, 898, 1026, 1165, 1313, 1470, 1635, 1807, 1985, 2167, 2352, 2537, 2723, 2905, 3084]
  list_unknown_fetal_growth_percentile_25 = [83, 106, 133, 166, 206, 252, 307, 370, 443, 525, 618, 723, 838, 964, 1102, 1251, 1410, 1579, 1757, 1942, 2134, 2330, 2531, 2733, 2935, 3135, 3333]
  list_unknown_fetal_growth_percentile_50 = [90, 114, 144, 179, 222, 272, 330, 398, 476, 565, 665, 778, 902, 1039, 1189, 1350, 1523, 1707, 1901, 2103, 2312, 2527, 2745, 2966, 3186, 3493, 3617]
  list_unknown_fetal_growth_percentile_75 = [98, 124, 155, 193, 239, 292, 355, 428, 512, 608, 715, 836, 971, 1118, 1279, 1453, 1640, 1838, 2047, 2266, 2492, 2723, 2959, 3195, 3432, 3664, 3892]
  list_unknown_fetal_growth_percentile_90 = [104, 132, 166, 207, 255, 313, 380, 458, 548, 650, 765, 894, 1038, 1196, 1368, 1554, 1753, 1664, 2187, 2419, 2659, 2904, 3153, 3403, 3652, 3897, 4135]
  list_unknown_fetal_growth_percentile_95 = [109, 138, 174, 217, 268, 328, 399, 481, 575, 682, 803, 938, 1087, 1251, 1429, 1622, 1828, 2046, 2276, 2516, 2764, 3018, 3277, 3538, 3799, 4058, 4312]
  list_unknown_fetal_growth_percentile_97_point_5 = [113, 144, 181, 225, 278, 340, 413, 497, 595, 705, 830, 970, 1125, 1295, 1481, 1682, 1897, 2126, 2367, 2619, 2880, 3148, 3422, 3697, 3973, 4247, 4515]

  # Create pandas data frames of growth charts
  cols = ['5', '10', '25', '50', '75', '90', '95']
  cols_unknown = ['2.5', '5', '10', '25', '50', '75', '90', '95', '97.5']
  df_female_fetal_growth_chart = pd.DataFrame(list(zip(list_female_fetal_growth_percentile_5, list_female_fetal_growth_percentile_10,
                             list_female_fetal_growth_percentile_25, list_female_fetal_growth_percentile_50, list_female_fetal_growth_percentile_75,
                            list_female_fetal_growth_percentile_90, list_female_fetal_growth_percentile_95)), 
                 index=list_gestational_age, columns = cols)
  df_male_fetal_growth_chart = pd.DataFrame(list(zip(list_male_fetal_growth_percentile_5, list_male_fetal_growth_percentile_10,
                             list_male_fetal_growth_percentile_25, list_male_fetal_growth_percentile_50, list_male_fetal_growth_percentile_75,
                            list_male_fetal_growth_percentile_90, list_male_fetal_growth_percentile_95)), 
                 index=list_gestational_age, columns = cols)
  df_unknown_fetal_growth_chart = pd.DataFrame(list(zip(list_unknown_fetal_growth_percentile_2_point_5, list_unknown_fetal_growth_percentile_5,
                                                       list_unknown_fetal_growth_percentile_10, list_unknown_fetal_growth_percentile_25, list_unknown_fetal_growth_percentile_50,
                                                       list_unknown_fetal_growth_percentile_75, list_unknown_fetal_growth_percentile_90, list_unknown_fetal_growth_percentile_95,
                                                       list_unknown_fetal_growth_percentile_97_point_5)), 
                 index=list_gestational_age, columns = cols_unknown)
  return(df_female_fetal_growth_chart, df_male_fetal_growth_chart, df_unknown_fetal_growth_chart)


def generate_trendline(x, y):
  import numpy as np
  x = [int(i) for i in x]
  x = np.array(x)
  y = [int(i) for i in y]
  y = np.array(y)
  idx = np.isfinite(x) & np.isfinite(y)
  ab = np.polyfit(np.log(x[idx]), y[idx], 1)
  print(ab)
  return(ab)


def define_percentile_trendlines(df, cols):
  d = {}
  list_gestational_age = [14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]
  for c in cols[1:]:
    x = list_gestational_age
    y = list(df[c])
    ab = generate_trendline(x, y)
    d[c] = ab
  return(d)


def generate_percentile_dicts():
  df_female_fetal_growth_chart, df_male_fetal_growth_chart, df_unknown_fetal_growth_chart = generate_fetal_growth_charts()
  dict_female_fetal_growth_chart = define_percentile_trendlines(df_female_fetal_growth_chart, list(df_female_fetal_growth_chart.columns))
  dict_male_fetal_growth_chart = define_percentile_trendlines(df_male_fetal_growth_chart, list(df_male_fetal_growth_chart.columns))
  dict_unknown_fetal_growth_chart = define_percentile_trendlines(df_unknown_fetal_growth_chart, list(df_unknown_fetal_growth_chart.columns))
  return(dict_female_fetal_growth_chart, dict_male_fetal_growth_chart, dict_unknown_fetal_growth_chart)


def evaluate_weight(ab, weight, gestational_age):
  a, b= ab[0], ab[1]
  expected_weight = (np.log(gestational_age)*a) + b
  print(expected_weight)
  if weight < expected_weight:
    return(True)
  return(False)


def calc_percentile(obs, expected_upper, expected_lower, upper_percentile, lower_percentile):
  # use interpolation to calculate the percentile
  percentile = upper_percentile - (upper_percentile-lower_percentile)*(expected_upper-obs)/(expected_upper - expected_lower)
  return(percentile)


def determine_range(n):
  i = int(n)
  if i-1 < n < i:
    return(i-1, i, i - n)
  return(i, i+1, n-i)


def calculate_expected_weight(df, upper_week, lower_week, remainder, percentile):
  percentile = str(percentile)
  if upper_week > 40:
    upper_limit = df.loc[40, percentile]
    lower_limit = df.loc[39, percentile]
    return(lower_limit + (upper_week - 40 - remainder) * (upper_limit-lower_limit))
  if lower_week < 14:
    upper_limit = df.loc[14, percentile]
    lower_limit = df.loc[15, percentile]
    return(lower_limit - (14 - lower_week + remainder) * (upper_limit-lower_limit))
  upper_limit = df.loc[upper_week, percentile]
  lower_limit = df.loc[lower_week, percentile]
  return(lower_limit + remainder * (upper_limit-lower_limit))


def determine_percentile(df, keys, upper_week, lower_week, remainder, weight, gestational_age):
  k = keys[0]
  expected_weight = calculate_expected_weight(df, upper_week, lower_week, remainder, k)
  if weight < expected_weight:
    return(float(k))
  prev_key = k
  for k in keys[1:]:
    expected_weight = calculate_expected_weight(df, upper_week, lower_week, remainder, k)
    if weight < expected_weight:
      expected_upper = calculate_expected_weight(df, upper_week, lower_week, remainder, k)
      expected_lower = calculate_expected_weight(df, upper_week, lower_week, remainder, prev_key)
      return(calc_percentile(weight, expected_upper, expected_lower, float(k), float(prev_key)))
    prev_key = k
  return(float(keys[-1]))


def calc_birth_weight_percentile(weight, gestational_age, gender):
  weight = float(weight)
  if weight < 500:   # convert oz to g
    weight = float(weight) * 28.3495
  gestational_age = float(gestational_age)/7  # convert days to weeks
  lower_week, upper_week, remainder = determine_range(gestational_age)
  if gender:  # make gender lowercase
    gender = gender.lower()
  df_female_fetal_growth_chart, df_male_fetal_growth_chart, df_unknown_fetal_growth_chart = generate_fetal_growth_charts()
  if gender == 'female':
    df = df_female_fetal_growth_chart
    keys = ['5', '10', '25', '50', '75', '90', '95']
    percentile = determine_percentile(df, keys, upper_week, lower_week, remainder, weight, gestational_age)
    return(percentile)
  elif gender == 'male':
    df = df_male_fetal_growth_chart
    keys = ['5', '10', '25', '50', '75', '90', '95']
    percentile = determine_percentile(df, keys, upper_week, lower_week, remainder, weight, gestational_age)
    return(percentile)
  df = df_unknown_fetal_growth_chart
  keys = ['2.5', '5', '10', '25', '50', '75', '90', '95', '97.5']
  percentile = determine_percentile(df, keys, upper_week, lower_week, remainder, weight, gestational_age)
  return(percentile)


def get_fetal_growth_percentiles(df):
  '''
  Calculate the fetal growth percentile of infants at delivery.
  
  @df       dataframe containing information on the delivery event
  @return   list of the fetal growth percentiles of infants in the input dataframe
  '''
  data = []
  for index, row in df.dropna(subset=['delivery_infant_birth_weight_oz']).iterrows():
    weight = row['delivery_infant_birth_weight_oz']
    gestational_age = row['gestational_days']
    gender = row['ob_hx_infant_sex']
    if gender is None:
      gender = 'unknown'
    if weight is None or math.isnan(weight):
      continue
    if gestational_age is None or math.isnan(gestational_age):
      continue
    data.append(calc_birth_weight_percentile(weight, gestational_age, gender))
  return(data)


def count_small_for_gestational_age(l):
  '''
  Count the number of infants that are small for gestational age (sga) verus normal size
  based upon their fetal growth percentile at delivery. SGA is defined as being in the
  bottom 10th percentile of fetal growth at delivery.
  
  @l        list of the fetal growth percentiles of infants in the input dataframe
  @return   dictionary of the count of infants that are small for gestational age or normal
  '''
  d = {'SGA': 0, 'normal': 0}
  for item in l:
    if float(item) <= 10:
      d['SGA'] += 1
    else:
      d['normal'] += 1
  return(d)
