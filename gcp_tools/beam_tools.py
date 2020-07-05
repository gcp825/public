#!/usr/bin/python3

#######################################################################################################
# ** REALLY IMPORTANT **
#  To avoid the possibility of mutating input data across parallel pipelines, any destructive
#  transforms herein (e.g. adding/removing/updating list items) are performed on COPIES of the input
#  data and not on the input data itself.
#
#  To effect this we use the python deepcopy function to create true independent copies. Using
#  regular functions (list, tuple, dict etc) to create copies or copying an object with y = x syntax
#  only creates shallow copies. These copies are partially composed of references back to the original
#  object: thus if the original object is modified this can impact the shallow copy and vice versa
#
#  Want to see what I mean? Try this in your python shell:
#
#  orig = ['a','b','c']
#  copy = orig
#  new = [] + copy
#  copy.pop(1)
#  print(str(orig) + '     ' + str(copy) + '     ' + str(new))
#
########################################################################################################

from copy import deepcopy
from hashlib import blake2b as b2b
import apache_beam as beam
import csv
import operator

#######################################################################################################
# DropFields - Specify numeric postional args of fields to drop
#            - All other fields retain their same relative position
#######################################################################################################

class DropFields(beam.PTransform):
    
    def __init__(self, *args):
        self.args = args
    
    def expand(self, pcoll):
      
        return (pcoll | 'Reformat' >> beam.ParDo(self.Drop(*self.args)))
    
    class Drop(beam.DoFn):
    
        def __init__(self, *args):
            self.args = args
        
        def process(self, listin):

            arglist = list(self.args)
            arglist.sort(reverse=True)
            listout = deepcopy(listin)
        
            for i in arglist: listout.pop(i)
            
            yield listout

#######################################################################################################
# KeepFields - Specify numeric postional args of fields to keep
#            - Specifying fields out of sequence will rearrange their relative position
#                e.g. for three fields 0,1,2, KeepFields(2,1,0) will reverse their order
#######################################################################################################
 
class KeepFields(beam.PTransform):
    
    def __init__(self, *args):
        self.args = args
    
    def expand(self, pcoll):
    
        return (pcoll | 'Reformat' >> beam.ParDo(self.Keep(*self.args)))

    class Keep(beam.DoFn):
    
        def __init__(self, *args):
            self.args = args
        
        def process(self, listin):
     
             listout = []

             for i in self.args:
                 if str(i).lower() == 'x':
                     listout = listout + ['']
                 else:   
                     listout = listout + [str(listin[i])]

             yield listout

#######################################################################################################
# AppendFields - Specify numeric postional args of fields to append to the end of the current list
#######################################################################################################
 
class AppendFields(beam.PTransform):
    
    def __init__(self, *args):
        self.args = args
    
    def expand(self, pcoll):
    
        return (pcoll | 'Append' >> beam.ParDo(self.Keep(*self.args)))

    class Keep(beam.DoFn):
    
        def __init__(self, *args):
            self.args = args
        
        def process(self, listin):
     
             listout = deepcopy(listin)

             for i in self.args:
                 if str(i).lower() == 'x':
                     listout = listout + ['']
                 else:                     
                     listout = listout + [str(listin[i])] 

             yield listout

#######################################################################################################
# XForm - Transforms a field in situ
#
# Fields - Specify keyword args of format: xn = transform    where...
#    - Specify kwargs of format: xn=transform
#        where...
#          -   n is the position within the input list of the field to transform
#          -   x is an alphabetic character (doesn't matter what provided xn is a unique kwarg)
#                e.g. a13 & b13 would perform iterative transforms of the 14th field in a list (index 13)
#          -   transform is a defined function or lambda expression
#######################################################################################################
             
class XForm(beam.PTransform):
    
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    
    def expand(self, pcoll):
      
        return (pcoll | 'Reformat' >> beam.ParDo(self.XFormer(**self.kwargs)))
 
    class XFormer(beam.DoFn):
    
        def __init__(self, **kwargs):
            self.kwargs = kwargs
        
        def process(self, listin):

            keylist = list(self.kwargs.keys())
            for i,key in enumerate(keylist): keylist[i] = int(key[1:])
                
            transforms = zip(keylist,list(self.kwargs.values()))
            listout = deepcopy(listin)
            
            for i,func in transforms: listout[i] = func(listout[i])
       
            yield listout 

#######################################################################################################
# XFormAppend - Using a specified field as input, transforms it & appends the result to the end of the
#               input list
#
#    - Specify kwargs of format: xn=transform
#        where...
#          -   n is the position within the input list of the field to transform
#          -   x is an alphabetic character (doesn't matter what provided xn is a unique kwarg)
#                e.g. a13 & b13 would perform separate transforms of the 14th field in a list (index 13)
#          -   transform is a defined function or lambda expression
#######################################################################################################
             
class XFormAppend(beam.PTransform):
    
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    
    def expand(self, pcoll):
      
        return (pcoll | 'Reformat' >> beam.ParDo(self.XFormer(**self.kwargs)))
 
    class XFormer(beam.DoFn):
    
        def __init__(self, **kwargs):
            self.kwargs = kwargs
        
        def process(self, listin):

            keylist = list(self.kwargs.keys())
            for i,key in enumerate(keylist): keylist[i] = int(key[1:])
                
            transforms = zip(keylist,list(self.kwargs.values()))
            listout = deepcopy(listin)
            
            for i,func in transforms: listout.append(func(listout[i]))
            
            yield listout 
 
#######################################################################################################
# Sort - Specify numeric postional args of fields to keep use to sort the input PCollection
#
#      - If you append a full stop to the end of a numeric positional argument this tells the sort to
#         sort it as a number, and not a string. Note that the numeric sort does not cope with
#         empty strings or None types - if sorting as a number, the repective field must be populated
#         with a number on all records. You can make this work in a clunky way at the moment by
#         transforming prior to the sort to add a defailt value (and remove this later).
#
#      - Specifying reverse=True will switch the sort order to descending. Note that all arguments
#        must have the same sort order you can't specify the sort order at individual field level
#
#  Both of these are weaknesses in the python sort function. It would need much more customisation than
#  I've got here to extend it to cater for this functionality.
#
#######################################################################################################             
             
class Sort(beam.PTransform):
    
    def __init__(self, *args, **kwargs):
        self.args = args
        self.args2 = [int(i) for i in args]
        self.kwargs = kwargs
    
    def expand(self, pcoll):
        
        def _normalise_list_of_lists(listin):
    
            for rec in listin:
                output = "|".join([str(x) for x in rec])
                yield output.split('|')
            
      
        return (
            pcoll | 'Parse Numerics'     >> beam.ParDo(self.ParseNumbers(*self.args))
                  | 'Combine Lists'      >> beam.combiners.ToList()
                  | 'Sort List'          >> beam.ParDo(self.SortList(*self.args2,**self.kwargs))
                  | 'Normalise Lists'    >> beam.FlatMap(_normalise_list_of_lists)
               )

    class ParseNumbers(beam.DoFn):
        
        def __init__(self, *args):
            self.args = args

        def process(self, listin):
 
            listout = deepcopy(listin)
                                                     
            for arg in self.args:
                                                     
                if type(arg) is float: 
                                                     
                    i = int(arg)
                    x = listout[i]
                    negative = True if x[0:1] == '-' else False
                    x = x.lstrip('-+')
                                                     
                    y = (float(x) if x.find('.') >= 0 
                                 and x.replace('.','').isdigit() is True 
                                 and (len(x) - len(x.replace('.',''))) == 1 
                    else int(x)   if x.isdigit() is True
                    else '')
                                                     
                    if type(y) is not str:
                        listout[i] = y if not negative else y*-1
                        
            yield listout


    class SortList(beam.DoFn):
    
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def process(self, listin):
 
            order = self.kwargs.get('reverse',False)
            listout = deepcopy(listin)
            listout.sort(key=operator.itemgetter(*self.args),reverse=order)
            yield listout

#######################################################################################################
# DistinctList - Implementation of beam.transforms.util.Distinct() for Python List PCollections
#
#  ** Probably a more efficient way to do this in a single function & keeping as a list throughout
#######################################################################################################             
             
class DistinctList(beam.PTransform):
    
    def expand(self, pcoll):
      
        return (
            pcoll | 'Convert to Rec'   >> beam.ToString.Iterables(delimiter='|')
                  | 'Dedupe'           >> beam.transforms.util.Distinct()
                  | 'Convert to List'  >> beam.ParDo(ConvertRecTo(list,'|'))
               )

#######################################################################################################
# Join - joins the primary PCollection in a pipeline to another PCollection (as a side input)
#
#    - Specify arguments as follows:
#
#      Arg 0 - the side input PCollection to join the main PCollection to
#      Arg 1 - the type of join to perform (modelled on standard SQL logic)... supply either:
#                'Inner', 'Left', 'Cross', 'Exists' ,'Not_Exists'
#
#              - 'Inner', 'Left', 'Cross' will return the attributes from both PCollections as a list
#              - 'Exists', 'Not_Exists' will return just those records from the main PCollection that
#                   exist/do not exist on the side input PCollection (as appropriate)
#
#    - Specify keyword arguments as follows:
#
#      main_key - a comma delimited text string specifying the list indexes of the items in the main
#                 PCollection that are to be used as join criteria
#      side_key - a comma delimited text string specifying the list indexes of the items in the side
#                 PCollection that are to be used as join criteria. These will be positionally matched
#                 to the arguments supplied in main_key i.e
#
#                 main_key = '1,2', side_key='3,0' would join main[1] to side[3] & main[2] to side[0]
#
#      key      - a comma delimited text string specifying the list indexes of the items in both the
#                 main and side PCollections that are to be used as join criteria. i.e. 'key' can be
#                 supplied INSTEAD of main_key and side_key if both PCollections share the same format
#                 (or different formats but with the join fields in the same positions)
#
#      keep     - an optional argument that if supplied should contain a comma delimited text string
#                 containing the list indexes of items in the result PCollection that should be
#                 retained. Invokes the beam_tools.KeepFields transform. Only used by Inner/Left/Cross
#                 modes
#
#######################################################################################################

class Join(beam.PTransform):
    
    def __init__(self, side_input, join, **kwargs):
        
        self.side_input = side_input
        self.join = join[0:1].upper()
        self.keep = [] if str(kwargs.get('keep','')) == '' else [int(x) for x in list(str(kwargs.get('keep')).split(','))]
        
        if str(kwargs.get('main_key','x')) != 'x': key = 'main_key'
        elif str(kwargs.get('key','x')) != 'x': key = 'key'
        else: key = 'side_key'
        
        self.main_key = [int(x) for x in str(kwargs.get(key,0)).split(',')]

        if str(kwargs.get('side_key','x')) != 'x': key = 'side_key'
        elif str(kwargs.get('key','x')) != 'x': key = 'key'
        else: key = 'main_key'
        
        self.side_key = [int(x) for x in str(kwargs.get(key,0)).split(',')]
        
     
    def expand(self, pcoll):

        def _join_key(listin,args):
            
            join_str = '~'.join([str(listin[i]) for i in args])
            join_key = join_str if len(join_str) <= 32 else b2b(join_str.encode('utf-8'),digest_size=16).hexdigest()
            return join_key

        def _prepare(side_input): return (_join_key(side_input,self.side_key),side_input)
               
        def _cross(main,side_input):
            
            for rec in side_input:
                yield deepcopy(main + rec)
                
        def _inner(main,side_input):
            
            main_join_key = _join_key(main,self.main_key)
            
            for rec in side_input:
                if main_join_key == rec[0]:
                    yield deepcopy(main + rec[1])
                    
        def _exists(main,side_input):
            
            main_join_key = _join_key(main,self.main_key)
            
            for rec in side_input:
                if main_join_key == rec[0]:
                    yield deepcopy(main)
                    break
                                    
        def _left(main,side_input):
 
            main_join_key = _join_key(main,self.main_key)
            no_match = []
            joined = False
             
            for rec in side_input:
                no_match = len(rec[1]) * ['']
                break
            
            for rec in side_input:

                if main_join_key == rec[0]:
                    joined = True
                    yield deepcopy(main + rec[1])
                    
            if not joined: yield deepcopy(main + no_match)
            
        def _not_exists(main,side_input):
 
            main_join_key = _join_key(main,self.main_key)
            joined = False
            
            for rec in side_input:
                if main_join_key == rec[0]:
                    joined = True
                    break
                
            if not joined: yield deepcopy(main)
            
#       Pipeline:

        side = (self.side_input | 'Prepare' >> beam.Map(_prepare)) if self.join in 'LINE' else self.side_input
            
        reformat = False if len(self.keep) == 0 or self.join in 'EN' else True
        
        join_func = (_inner      if self.join == 'I' else
                     _left       if self.join == 'L' else
                     _exists     if self.join == 'E' else
                     _not_exists if self.join == 'N' else
                     _cross)
                    
        if reformat:
            return (pcoll | 'Join' >> beam.FlatMap(join_func, beam.pvalue.AsIter(side))
                          | 'Keep' >> KeepFields(*self.keep))
        else:
            return (pcoll | 'Join' >> beam.FlatMap(join_func, beam.pvalue.AsIter(side)))

#######################################################################################################
# Lookup - joins the primary PCollection in a pipeline to another PCollection (as a side input)
#        - specifically for use where the join key value on the side input is unique (as would
#          normally be the case in a lookup) as this allows the side input to be processed as a
#          Dictionary... which is a far quicker method of joining than having to iterate through the
#          entire side input
#
#    - Specify arguments as follows:
#
#      Arg 0 - the side input PCollection to join the main PCollection to
#      Arg 1 - the type of join to perform (modelled on standard SQL logic)... supply either:
#                'Inner', 'Left', 'Exists' ,'Not_Exists'
#
#              - 'Inner', 'Left' will return the attributes from both PCollections as a list
#              - 'Exists', 'Not_Exists' will return just those records from the main PCollection that
#                   exist/do not exist on the side input PCollection (as appropriate)
#
#                Note there is no Cross join option when using Lookup - use Join for this
#
#    - Specify keyword arguments as follows:
#
#      side_val - a comma delimited text string specifying the list indexes of the items in the side
#                 input that are to be returned in the event of a successful join
#      main_key - a comma delimited text string specifying the list indexes of the items in the main
#                 PCollection that are to be used as join criteria
#      side_key - a comma delimited text string specifying the list indexes of the items in the side
#                 PCollection that are to be used as join criteria. These will be positionally matched
#                 to the arguments supplied in main_key i.e
#
#                 main_key = '1,2', side_key='3,0' would join main[1] to side[3] & main[2] to side[0]
#
#      key      - a comma delimited text string specifying the list indexes of the items in both the
#                 main and side PCollections that are to be used as join criteria. i.e. 'key' can be
#                 supplied INSTEAD of main_key and side_key if both PCollections share the same format
#                 (or different formats but with the join fields in the same positions)
#
#      keep     - an optional argument that if supplied should contain a comma delimited text string
#                 containing the list indexes of items in the result PCollection that should be
#                 retained. Invokes the beam_tools.KeepFields transform. Only used by Inner/Left modes.
#
#######################################################################################################

class Lookup(beam.PTransform):
    
    def __init__(self, side_input, join, **kwargs):
        
        self.side_input = side_input
        self.join = join[0:1].upper()
        self.keep = [] if str(kwargs.get('keep','')) == '' else [int(x) for x in list(str(kwargs.get('keep')).split(','))]
        
        if str(kwargs.get('main_key','x')) != 'x': key = 'main_key'
        elif str(kwargs.get('key','x')) != 'x': key = 'key'
        else: key = 'side_key'
        
        self.main_key = [int(x) for x in str(kwargs.get(key,0)).split(',')]

        if str(kwargs.get('side_key','x')) != 'x': key = 'side_key'
        elif str(kwargs.get('key','x')) != 'x': key = 'key'
        else: key = 'main_key'
        
        self.side_key = [int(x) for x in str(kwargs.get(key,0)).split(',')]
        
        self.side_val = [] if str(kwargs.get('side_val','')) == '' else [int(x) for x in list(str(kwargs.get('side_val')).split(','))]
        
        self.no_match = len(self.side_val) * ['']
        
        
    def expand(self, pcoll):

        def _key(listin,args):
            
            join_str = '~'.join([str(listin[i]) for i in args])
            join_key = join_str if len(join_str) <= 32 else b2b(join_str.encode('utf-8'),digest_size=16).hexdigest()
            return join_key
        
        def _prepare(listin):        
  
            listout = []
            val = []
            
            key = _key(listin,self.side_key)
            for i in self.side_val: val = val + [str(listin[i])]
            
            listout = listout + [key] + val
            return listout
        
        def _convert(listin):
            
            key = deepcopy(listin[0])
            val = deepcopy(listin)
            val.pop(0)
            return (key,val)
        
        def _inner(main,side_input):
 
            main_join_key = _key(main,self.main_key)            
            sidelist = side_input.get(main_join_key,'x')
            
            if type(sidelist) is list:
                listout = [] + deepcopy(main) + deepcopy(sidelist)
                yield listout
                
        def _exists(main,side_input):
 
            main_join_key = _key(main,self.main_key)
            sidelist = side_input.get(main_join_key,'x')
            
            if type(sidelist) is list:
                yield deepcopy(main)

        def _left(main,side_input):
 
            main_join_key = _key(main,self.main_key)            
            sidelist = side_input.get(main_join_key,self.no_match)
            
            listout = [] + deepcopy(main) + deepcopy(sidelist)
            yield listout
            
        def _not_exists(main,side_input):
 
            main_join_key = _key(main,self.main_key)            
            sidelist = side_input.get(main_join_key,'x')
            
            if type(sidelist) is str:
                yield deepcopy(main)
      
#       Pipeline:

        side = (self.side_input | 'Prepare'  >> beam.Map(_prepare)
                                | 'Distinct' >> DistinctList()
                                | 'Convert'  >> beam.Map(_convert))
        
        reformat = False if len(self.keep) == 0 or self.join in 'EN' else True
        
        join_func = (_inner      if self.join == 'I' else
                     _exists     if self.join == 'E' else
                     _not_exists if self.join == 'N' else
                     _left)
                    
        if reformat:
            return (pcoll | 'Join' >> beam.FlatMap(join_func, beam.pvalue.AsDict(side))
                          | 'Keep' >> KeepFields(*self.keep))
        else:
            return (pcoll | 'Join' >> beam.FlatMap(join_func, beam.pvalue.AsDict(side)))
        
            
#######################################################################################################
# GenerateSKs - Given an integer starting key arg, this will iterate through a PCollection,
#                 incrementing the new SK value by the interval value each time and prepending the SK 
#                 to the start of each PCollection 'record'
#######################################################################################################  

class GenerateSKs(beam.PTransform):
    
    def __init__(self, start_sk=1, interval=1):
        self.start_sk = start_sk
        self.interval = interval                                           
    
    def expand(self, pcoll):
        
        return (pcoll | 'Add Keys ' >> beam.ParDo(self.PrependSKs(self.start_sk,self.interval)))

    class PrependSKs(beam.DoFn):
    
        def __init__(self, start_sk, interval):
            self.next_sk = start_sk
            self.interval = interval
        
        def process(self, listin):
    
            sk = [] + [str(self.next_sk)]
            yield sk + deepcopy(listin)
            self.next_sk = self.next_sk + self.interval

#######################################################################################################
# Count - the direct equivalent of the sql count/group by function - when supplied with list indexes of
#         items in the input PCollection, this will determine the distinct value combinations of the
#         data represented by those indexes and tally the number of records with those values
#
#         i.e (PColl | 'Count' >> beam_tools.Count(0,1))
#
#             is the direct equivalent of:
#
#             SELECT Column_0, Column_1, Count(*) FROM PColl GROUP BY Column_0, Column_1
#
# ** Could be expanded with a filter option as the equivalent of HAVING COUNT(*)...
# ** Possible to add COUNT(DISTINCT(column)) functionality?
#
#######################################################################################################

class Count(beam.PTransform):
    
    def __init__(self, *args):
        self.args = args
    
    def expand(self, pcoll):
      
        return (
            pcoll | 'Reformat'           >> KeepFields(*self.args)
                  | 'Convert to Str'     >> beam.ToString.Iterables(delimiter='|')
                  | 'Count'              >> beam.combiners.Count.PerElement()
                  | 'Convert to List'    >> beam.Map(lambda x: x[0].split('|') + [str(x[1])])
               )
 
#######################################################################################################
# SplitAttributes - The first argument must be the delimiter character you wish to split on            
#                 - Subsequent arguments should be numeric postions of fields to split
#                 - Fields will be split in situ, i.e. fields following those that are split may be
#                     pushed further out   
#######################################################################################################

class SplitAttributes(beam.PTransform):
    
    def __init__(self, split_char, *args):
        self.split_char = split_char
        self.args = args
    
    def expand(self, pcoll):
      
        return (pcoll | 'Split Attributes' >> beam.ParDo(self.SplitAttribute(self.split_char, *self.args)))

    class SplitAttribute(beam.DoFn):
    
        def __init__(self, split_char, *args):
            self.split_char = split_char
            self.args = args
        
        def process(self, listin):
    
            arglist = list(self.args)
            arglist.sort(reverse=True)
            listout = deepcopy(listin)
                                                     
            for i in arglist:
                parts = listout[i].split(self.split_char)
                listout.pop(i)
                for p in reversed(parts): listout.insert(i,p.strip())    
 
            yield listout
            
#######################################################################################################
# Normalise - Takes a list of values and normalises to multiple output lists
#           - The first argument should be the number of output lists to normalise to
#           - Subsequent numeric arguments reflect...
#               - the fields to normalise to output list 1 (in order)
#               - the fields to normalise to output list 2 (in order) etc. as per no. lists specified
#           - Any list items not specified in the subsequent numeric arguments will be prepended to
#             all output lists
#           - Specifiying the kwarg blanks='n' will result in any output lists where all of the 
#             specified list items are empty strings NOT being output
#######################################################################################################

class Normalise(beam.PTransform):
    
    def __init__(self, outputs, *args, **kwargs):
        self.outputs = outputs
        self.args = args
        self.kwargs = kwargs
    
    def expand(self, pcoll):
      
        return (pcoll | 'Normalise List' >> beam.ParDo(self.Normaliser(self.outputs,*self.args,**self.kwargs)))

    class Normaliser(beam.DoFn):
    
        def __init__(self, outputs, *args, **kwargs):
            self.outputs = outputs
            self.args = args
            self.kwargs = kwargs
        
        def process(self, listin):
            
            blanks = self.kwargs.get('blanks','y').lower()
            commonlist = []
                                                     
            for i in range(len(listin)):
                if i not in self.args:
                    commonlist.append(listin[i])

            limit = len(self.args) // self.outputs
            counter = limit + 1
            
            for i in self.args:
                
                if counter > limit:
                    templist = []
                    counter = 1
                    
                templist = templist + [listin[i]]
                counter = counter + 1
                
                if counter > limit:
                    if blanks != 'n' or len(''.join([str(x) for x in templist])) > 0:
                        listout = [] + commonlist + templist
                        yield listout
                        
#######################################################################################################
# First - Limits the PCollection to the first n records specified (supplied as the only arg)
#######################################################################################################
            
class First(beam.PTransform):
    
    def __init__(self, limit):
        self.num_of_recs = 1
        self.limit = limit
    
    def expand(self, pcoll):
        
        def _first(listin):
       
            if self.num_of_recs <= self.limit:
                self.num_of_recs = self.num_of_recs + 1
                yield deepcopy(listin)
        
        return (pcoll | 'Filter' >> beam.FlatMap(_first))


#######################################################################################################
# SwitchDelimiters - Supply two args: old delimiter and new delimiter
#######################################################################################################

class SwitchDelimiters(beam.DoFn):

    def __init__(self, old, new):
        self.old = old
        self.new = new

    def process(self, linein):
        
        rec = list(csv.reader([linein.replace(self.new,'')],delimiter=self.old,quotechar='"',skipinitialspace=True))[0]
        yield self.new.join(map(str,rec)).replace('"','')

#######################################################################################################
# ConvertRecTo - Converts a delimited record to a Python format of your choosing
#                    i.e supply the list, tuple or dict function, plus your field delimiter
#######################################################################################################

class ConvertRecTo(beam.DoFn):
    
    def __init__(self, func, delimiter):
        self.func = func
        self.delimiter = delimiter
      
    def process(self, linein):
        rec = linein.split(self.delimiter)
        yield self.func(rec)

#######################################################################################################
# Copy - Duplicates a PCollection, creating n output copies (n has a max value of 5 & defaults to 2) 
#      
# Example usage:
#
#  >> beam.ParDo(beam_tools.Copy()).with_outputs(beam_tools.Copy.x1, beam_tools.Copy.x2)
#  >> beam.ParDo(beam_tools.Copy(3)).with_outputs(beam_tools.Copy.x1, beam_tools.Copy.x2, beam_tools.Copy.x3)
#
# Assuming the copies were produced as output from a PCollection 'P' they can be referenced as:
#   P.x1, P.x2, P.x3
#
#######################################################################################################

class Copy(beam.DoFn):
    
    x1, x2, x3, x4, x5 = ['x1','x2','x3','x4','x5']
    
    def __init__(self, copies=2):
        self.copies = copies
       
    def process(self, datain):
        
        yield beam.pvalue.TaggedOutput(self.x1, datain)     
        yield beam.pvalue.TaggedOutput(self.x2, datain)
        
        if self.copies > 2:
            yield beam.pvalue.TaggedOutput(self.x3, datain)
        if self.copies > 3:
            yield beam.pvalue.TaggedOutput(self.x4, datain)
        if self.copies > 4:
            yield beam.pvalue.TaggedOutput(self.x5, datain)          
