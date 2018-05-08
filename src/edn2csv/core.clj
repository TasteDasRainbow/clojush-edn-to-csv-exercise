(ns edn2csv.core
  (require [clojure.core.reducers :as r]
           [clojure.edn :as edn]
           [clojure.java.io :as io]
           [clojure.pprint :as pp]
           [clojure.set :as set]
           [iota]
           [me.raynes.fs :as fs])
  (:gen-class))

; The header line for the Individuals CSV file
(def individuals-header-line "UUID:ID(Individual),Generation:int,Location:int,:LABEL")
(def parentof-edges-header-line ":START_ID(Individual),GeneticOperator,:END_ID(Individual),:TYPE")

; atomic holder
(def sem-error (atom {}))

; Ignores (i.e., returns nil) any EDN entries that don't have the
; 'clojure/individual tag.
(defn individual-reader
    [t v]
    (when (= t 'clojush/individual) v))

; I got this from http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html
; It prints in a way that avoids weird interleaving of lines and items.
; In several ways it would be better to use a CSV library like
; clojure.data.csv, but that won't (as written) avoid the interleaving
; problems, so I'm sticking with this approach for now.
(defn safe-println [output-stream & more]
  (.write output-stream (str (clojure.string/join "," more) "\n")))

(defn print-single-parent-edge [out-file child parent]
    (apply safe-println out-file (concat [parent] child)))

; This prints out the relevant fields to the CSV filter
; and then returns 1 so we can count up how many individuals we processed.
; (The counting isn't strictly necessary, but it gives us something to
; fold together after we map this across the individuals; otherwise we'd
; just end up with a big list of nil's.)
(defn print-individual-to-csv
  [individual-out-file line]
  (as-> line $
    (map $ [:uuid :generation :location])
    (concat $ ["Individual"])
    (apply safe-println individual-out-file $))
  1)

(defn print-parentOf-edges-to-csv
  [parentof-edges-out-file line]
 (as-> line $
      (map $ [:genetic-operators :uuid])
      (concat $ ["PARENT_OF"])
      (doall (map (partial print-single-parent-edge parentof-edges-out-file $)(line :parent-uuids)))))

(defn print-individual-and-parentOf-to-csv
  [individual-out-file parentof-edges-out-file line]
  (do 
   (swap! sem-error (set/union @sem-error #{line}))
   (print-individual-to-csv individual-out-file line)
   (print-parentOf-edges-to-csv parentof-edges-out-file line)
   1))
      

(defn build-csv-filename
    [edn-filename type]
    (str (fs/parent edn-filename)
         "/"
         (fs/base-name edn-filename ".edn")
         "_"
         type
         ".csv"))

(defn edn->csv-reducers [edn-file]
  (with-open [individual-out-file (io/writer (build-csv-filename edn-file "Individual"))
             parentof-edges-out-file (io/writer (build-csv-filename edn-file "ParentOf_edges"))]
    (safe-println individual-out-file individuals-header-line)
    (safe-println parentof-edges-out-file  parentof-edges-header-line)
    (->>
      (iota/seq edn-file)
      (r/map (partial edn/read-string {:default individual-reader}))
      (r/filter identity)
      (r/map (partial print-individual-and-parentOf-to-csv individual-out-file parentof-edges-out-file))
      (r/fold +))))


(defn -main
    [edn-filename]
    (time
        (edn->csv-reducers edn-filename))
    ; Necessary to get threads spun up by `pmap` to shutdown so you get
    ; your prompt back right away when using `lein run`.
    (shutdown-agents))
