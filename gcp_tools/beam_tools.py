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
########################################################################################################

from copy import deepcopy
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
        
            for i in range(len(arglist)): listout.pop(arglist[i])
            
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
             for i in range(len(self.args)):
                 if str(self.args[i]) in ('x','X'):
                     listout = listout + ['']
                 else:                     
                     listout = listout + [str(listin[self.args[i]])]                                  

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
             for i in range(len(self.args)):
                 if str(self.args[i]) in ('x','X'):
                     listout = listout + ['']
                 else:                     
                     listout = listout + [str(listin[self.args[i]])]                                  

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
            listout = deepcopy(listin)
            transforms = {}
         
            for i in range(len(keylist)):
                transforms.update({int(keylist[i][1:]) : self.kwargs[keylist[i]]})
            keylist = list(transforms.keys())
         
            for i in range(len(keylist)):
                func = transforms[keylist[i]]
                oldval = listout[keylist[i]]
                newval = func(oldval)
                listout.pop(keylist[i])
                listout.insert(keylist[i],newval)                             

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
     
            keylist = []
            transforms = list(self.kwargs.values())
            listout = deepcopy(listin)
         
            for k in list(self.kwargs.keys()):
                keylist = keylist + [int(k[1:])]
         
            for i in range(len(keylist)):
                func = transforms[i]
                oldval = listout[keylist[i]]
                newval = func(oldval)
                listout.append(newval)
 
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
      
        return (
            pcoll | 'Parse Numerics'     >> beam.ParDo(self.ParseNumbers(*self.args))
                  | 'Combine Lists'      >> beam.combiners.ToList()
                  | 'Sort List'          >> beam.ParDo(self.SortList(*self.args2,**self.kwargs))
                  | 'Normalise Lists'    >> beam.FlatMap(normalise_list_of_lists)
               )

    class ParseNumbers(beam.DoFn):
        
        def __init__(self, *args):
            self.args = args

        def process(self, listin):
 
            arglist = list(self.args)
            listout = deepcopy(listin)
            
            for i in range(len(arglist)):
                
                if type(arglist[i]) is float:
                    
                    idx = int(arglist[i])               
                    sign = listout[idx][0:1]
                    tmp = listout[idx].lstrip('-+')
                
                    if tmp.find('.') >= 0 and tmp.replace('.','').isdigit() is True and (len(tmp) - len(tmp.replace('.',''))) == 1:
                        repl = float(tmp)
                    elif tmp.isdigit() is True:
                        repl = int(tmp)
                    else:
                        repl = ''
                    
                    if type(repl) is not str:
                        if sign == '-': repl = (repl * -1)
                        listout[idx] = repl                

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
#  ** Probably a more eficient way to do this in a single function & keeping as a list throughout
#######################################################################################################             
             
class DistinctList(beam.PTransform):
    
    def expand(self, pcoll):
      
        return (
            pcoll | 'Convert to Rec'   >> beam.ToString.Iterables(delimiter='|')
                  | 'Dedupe'           >> beam.transforms.util.Distinct()
                  | 'Convert to List'  >> beam.ParDo(ConvertRecTo(list,'|'))
               )

#######################################################################################################
# GenerateSKs - Given an integer starting key arg, this will iterate through a PCollection,
#                 incrementing the new SK value by one each time and prepending the SK to the start of
#                 each PCollection 'record'
#             - Could easily be amended to increment to a specified interval
#######################################################################################################  

class GenerateSKs(beam.PTransform):
    
    def __init__(self, start_sk):
        self.start_sk = start_sk
    
    def expand(self, pcoll):
      
        return (
            pcoll | 'Combine Lists'      >> beam.combiners.ToList()
                  | 'Add Keys '          >> beam.ParDo(self.PrependSKs(self.start_sk))
                  | 'Normalise Lists'    >> beam.FlatMap(normalise_list_of_lists)
               )
 
    class PrependSKs(beam.DoFn):
    
        def __init__(self, start_sk):
            self.start_sk = start_sk
        
        def process(self, listin):
    
            sk = self.start_sk
            listout = []
        
            for rec in listin:
                new_rec = [str(sk)] + rec
                listout.append(new_rec)
                sk = sk + 1

            yield listout

#######################################################################################################
# ApplyFKs - Append a foreign key value to the end of a PCollection 'record' and drop the associated  
#            natural key value(s) from the PCollection
#
#          - First argument is a PCollection of tuples containing a key/value lookup pairs
#              - the key should be the 'natural' key i.e. the data field(s) you want to replace with
#                a foreign key reference
#              - the value should be the foreign key you want to replace the natural fields with
#          - Subsequent arguments should be numeric position operators identifying the field(s) in
#              the input PCollection to be used to join to the Lookup Key
#
#          If a foreign key relates to a compound natural key of multiple fields, all of these fields
#            should be specified as arguments. The contents of these fields will be concatenated in the
#            order specified, with a tab between each field. The key value in the lookup will have to
#            be similarly prepared (you can use the CreateLookup class to do this).
#
#  ** Could easily be amended to insert the FK in a specific position
#  ** Uses SideInputs joining AsDict with FlatMap. This works well for smaller lookups, but
#       is there a better way to do this, particularly for larger datasets?
#######################################################################################################

class ApplyFKs(beam.PTransform):
    
    def __init__(self, lookup, *args):
        self.lookup = lookup
        self.args = args
    
    def expand(self, pcoll):
      
        return (pcoll | 'Apply FK' >> beam.FlatMap(apply_fk, beam.pvalue.AsDict(self.lookup), self.args))

def apply_fk(listin, lookup, args):

    arglist = list(args)
    listout = deepcopy(listin)
    key = listout[arglist[0]]
    
    for i in range(1,len(arglist)):
        key = key + '\t' + str(listout[arglist[i]])
   
    arglist.sort(reverse=True)
    for i in range(len(arglist)):
        listout.pop(arglist[i])       
        
    if len(key) == 0: listout = listout + [key]
    else: listout = listout + [lookup.get(key,'')]
            
    yield listout

#######################################################################################################
# AppendLookupVals - as per ApplyFKs except:
#     - Fields in the input PCollection that match to the lookup key are not removed from the output
#     - Multiple fields (tab separated) in the lookup value are split into individual output fields
#
#  ** Actually, looks like I've forgotten to code that multiple tab separated fields thing!
#######################################################################################################

class AppendLookupVals(beam.PTransform):
    
    def __init__(self, lookup, *args):
        self.lookup = lookup
        self.args = args
    
    def expand(self, pcoll):
      
        return (pcoll | 'Apply Lookup Val' >> beam.FlatMap(append_lookup_val, beam.pvalue.AsDict(self.lookup), self.args))

def append_lookup_val(listin, lookup, args):

    arglist = list(args)
    listout = deepcopy(listin)
    key = listout[arglist[0]]
    for i in range(1,len(arglist)):
        key = key + '\t' + str(listout[arglist[i]])
        
    if key in lookup:
        listout = listout + [lookup[key]]   
        yield listout

#######################################################################################################
# CreateLookup - Takes a PCollection and converts to a distinct set of tuple key/value pairs to be
#                used as a side input to the ApplyFKs or AppendLookupVals transform
#
#          - First argument is the 'split position' - the position in which the subsequent positional
#            arguments should be sliced to define which fields should form the key and which the value
#
#          - Subsequent arguments should be numeric position operators identifying the field(s) to
#            form the lookup in key, then value order.
#
#          - Multiple fields on the key or value side (as defined by the 'split position' (as it
#            relates to the number of subsequent arguments)) will be concatenated together,
#            delimited by tabs. e.g.
#
#            1,0,1      creates a key/value lookup of field0 -- field1
#            1,0,1,2    creates a key/value lookup of field0 -- field1 + tab + field2
#            2,3,2,0,1  creates a key/value lookup of field3  + tab + field2 -- field0 + tab + field1
#
# ** Can probably use DistinctLists instead and remove the string conversion
#
#######################################################################################################

class CreateLookup(beam.PTransform):
    
    def __init__(self, split_pos, *args):
        self.split_pos = split_pos
        self.args = args
    
    def expand(self, pcoll):
      
        return (
            pcoll | 'Reformat'           >> beam.ParDo(KeepFields.Keep(*self.args))
                  | 'Convert to Rec'     >> beam.ToString.Iterables(delimiter='|')
                  | 'Distinct'           >> beam.transforms.util.Distinct()
                  | 'Prep KeyVal Pair'   >> beam.ParDo(self.SplitAndConcat(self.split_pos))
                  | 'Convert to Tuple'   >> beam.ParDo(ConvertRecTo(tuple,'|'))
               )
 
    class SplitAndConcat(beam.DoFn):
    
        def __init__(self, split_pos):
            self.split_pos = split_pos
        
        def process(self, linein):
    
            rec = linein.split('|')
            yield "\t".join([str(x) for x in rec[0:self.split_pos]]) + '|' + "\t".join([str(x) for x in rec[self.split_pos:]])
 
#######################################################################################################
# CreateCountLookup - As per CreateLookup, except that here the specified arguments are the field(s)
#                     to form the key part of the key/value pair only.
#
#                     The value part of the key/value pair is calculated as the number of 'records'
#                     in the pcollection with that key or keys i.e. it's a COUNT(*)/GROUP BY
#
# ** Can probably remove the string conversion if Count works with Lists
#
#######################################################################################################

class CreateCountLookup(beam.PTransform):
    
    def __init__(self, *args):
        self.args = args
    
    def expand(self, pcoll):
      
        return (
            pcoll | 'Reformat'           >> beam.ParDo(KeepFields.Keep(*self.args))
                  | 'Convert to Rec #1'  >> beam.ToString.Iterables(delimiter='|')
                  | 'Count'              >> beam.combiners.Count.PerElement()
                  | 'Convert to Rec #2'  >> beam.ToString.Kvs(delimiter = '|')
                  | 'Prep KeyVal Pair'   >> beam.ParDo(self.SplitAndConcat(len(self.args)))
                  | 'Convert to Tuple'   >> beam.ParDo(ConvertRecTo(tuple,'|'))
               )
 
    class SplitAndConcat(beam.DoFn):
    
        def __init__(self, split_pos):
            self.split_pos = split_pos
        
        def process(self, linein):
    
            rec = linein.split('|')
            yield "\t".join([str(x) for x in rec[0:self.split_pos]]) + '|' + "\t".join([str(x) for x in rec[self.split_pos:]])     

#######################################################################################################
# SplitAttributes - The first argument must be the delimier character you wish to split on            
#                 - Subsequent arguments should be numeric postionas of fields to split
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
 
            for i in range(len(arglist)):
                parts = listout[arglist[i]].split(self.split_char)
                listout.pop(arglist[i])
                for p in reversed(range(len(parts))):
                    listout.insert(arglist[i], parts[p].strip())
 
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
            
            for i in range(len(self.args)):
                
                if counter > limit:
                    templist = []
                    counter = 1
                    
                templist = templist + [listin[self.args[i]]]
                counter = counter + 1
                
                if counter > limit:
                    if blanks != 'n' or len(''.join([str(x) for x in templist])) > 0:
                        listout = [] + commonlist + templist
                        yield listout

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
        
#######################################################################################################
# normalise_list_of_lists - takes a list of lists and normalises to a PCollection record per inner list
#######################################################################################################

def normalise_list_of_lists(listin):
    
    for rec in listin:
        output = "|".join([str(x) for x in rec])
        yield output.split('|')
