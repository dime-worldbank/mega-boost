# Databricks notebook source
!pip install pwlf

# COMMAND ----------

import pwlf
import pandas as pd
import numpy as np
import math
from itertools import product
from scipy.signal import find_peaks

class TrendDetector:
    def __init__(self, max_segments=3, threshold=0.05):
        self.max_segments = max_segments
        self.threshold = threshold

    @staticmethod
    def calculate_bic(ssr, n_data_points, n_segments):
        k = (2 * n_segments) + (n_segments - 1)
        # BIC formula
        bic = n_data_points * np.log(ssr / n_data_points) + k * np.log(n_data_points)
        return bic

    def find_local_maxima_years(self, x, y):
        peaks = find_peaks(y)[0]
        valleys = find_peaks([-i for i in y])[0]
        extrema_indices = set(peaks).union(set(valleys))
        local_extrema = [x[i] for i in range(len(x)) if i in extrema_indices]
        return local_extrema

    def is_valid_fit(self, model):
        p_values = model.p_values()
        breakpoints = [int(x) for x in model.fit_breaks]

        # Check if the breakpoints are not too close to each to other
        if len(breakpoints) != len(set(breakpoints)):
            return False

        # Check all the knots are significant
        for p_val in model.p_values()[1:]:
            if p_val > self.threshold:
                return False
        return True

    def _find_preliminary_model(self, x, y):
        """Identifies the best segment count and approximate breakpoint locations."""
        best_prelim_model = None
        best_seg_count = 0
        
        for n_seg in range(1, self.max_segments + 1):
            if len(x) - n_seg <= 1:
                break
            try:
                model = pwlf.PiecewiseLinFit(x, y)
                model.fit(n_seg, seed=42)
                
                # Use your p-value and proximity checks here
                if self.is_valid_fit(model):
                    best_prelim_model = model
                    best_seg_count = n_seg
            except Exception as e:
                print(f"Fit failed for {n_seg} segments: {e}")
                continue
                
        return best_prelim_model, best_seg_count

    def _refine_to_narrative_milestones(self, x, y, prelim_model, seg_count):
        """Snaps approximate breakpoints to years/extrema and picks the best BIC."""
        local_extrema = self.find_local_maxima_years(x, y)
        raw_breaks = prelim_model.fit_breaks
        neighbor_sets = []

        for b in raw_breaks:
            low, high = math.floor(b), math.ceil(b)
            
            # Priority logic: if a neighbor is a peak/valley, lock to it
            if low == high:
                neighbor_sets.append([low])
            elif low in local_extrema:
                neighbor_sets.append([low])
            elif high in local_extrema:
                neighbor_sets.append([high])
            else:
                neighbor_sets.append([low, high])

        best_final_model = None
        min_bic = np.inf

        for candidate in product(*neighbor_sets):
            # Validation: must be unique and strictly increasing
            if len(candidate) != len(set(candidate)):
                continue
                
            model = pwlf.PiecewiseLinFit(x, y)
            ssr = model.fit_with_breaks(candidate)
            current_bic = self.calculate_bic(ssr, len(x), seg_count)
            
            if current_bic < min_bic:
                min_bic = current_bic
                best_final_model = model
                
        return best_final_model

    def fit_best_model(self, x, y):
        # Step 1: Find the best 'rough' fit
        prelim_model, seg_count = self._find_preliminary_model(x, y)
        
        if prelim_model is None:
            return None
            
        # Step 2: Apply the constraints and find the best final fit
        final_model = self._refine_to_narrative_milestones(x, y, prelim_model, seg_count)
        
        return final_model
    
    def extract_trend(self, x, y, metadata={}):
        """
        Main entry point: Fits the model and returns a list of 
        significant breakpoints as dictionaries.
        """
        model = self.fit_best_model(x, y)
        if model is None:
            return []
        p_values = model.p_values()
        slopes = np.cumsum(model.beta[1:])
        breaks = model.fit_breaks
        segments = []
        for i in range(0, len(breaks)-1):
            start_year = breaks[i]
            end_year = breaks[i+1]
            start_index = np.searchsorted(x, start_year)
            end_index = np.searchsorted(x, end_year)
            start_index = min(start_index, len(y) - 1)
            end_index = min(end_index, len(y) - 1)
            segment = {
                'start_value': y[start_index],
                'end_value': y[end_index],
                "start_year": start_year,
                "end_year": end_year,
                'slope': slopes[i],
                "p_value": p_values[i],
            }
            segments.append(segment)

        return segments

# COMMAND ----------

class InsightExtractor:
    def __init__(self, X, Y):
        self.X = X
        self.Y = Y
        self.trend_detector = TrendDetector()

    def get_volatility(self):
        # Returns the CV (%)
        return (self.Y.std() / self.Y.mean()) * 100

    def get_structural_segments(self):
        return self.trend_detector.extract_trend(self.X, self.Y)

    def extract_full_suite(self):
        # Returns a dictionary ready for your Delta Table
        return {
            "cv_value": self.get_volatility(),
            "segments": self.get_structural_segments(),
        }
