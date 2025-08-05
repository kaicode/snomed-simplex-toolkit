package org.snomed.simplex.util;

import org.snomed.simplex.weblate.domain.CsvTranslationRow;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for streaming CSV data from a file without loading all rows into memory.
 */
public class CsvStreamReader implements AutoCloseable {
    
    private BufferedReader reader;
    private final String filePath;
    private final List<String> headers;
    private final int conceptCodeIndex;
    private final int translatedTermIndex;
    private final int commentIndex;
    private int currentLineNumber = 1; // Start at 1 since we've already read the header
    
    public CsvStreamReader(String filePath, String conceptCodeColumn, String translatedTermColumn, String commentColumn) 
            throws IOException {
        
        this.filePath = filePath;
        this.reader = new BufferedReader(new FileReader(filePath, StandardCharsets.UTF_8));
        
        // Read and parse header row
        String headerLine = reader.readLine();
        if (headerLine == null) {
            throw new IOException("CSV file is empty");
        }
        
        this.headers = parseCsvLine(headerLine);
        this.conceptCodeIndex = headers.indexOf(conceptCodeColumn);
        this.translatedTermIndex = headers.indexOf(translatedTermColumn);
        this.commentIndex = commentColumn != null ? headers.indexOf(commentColumn) : -1;
        
        if (conceptCodeIndex == -1) {
            throw new IOException("Concept code column '" + conceptCodeColumn + "' not found in CSV headers");
        }
        if (translatedTermIndex == -1) {
            throw new IOException("Translated term column '" + translatedTermColumn + "' not found in CSV headers");
        }
    }
    
    /**
     * Reads the next valid translation row from the CSV file.
     * @return CsvTranslationRow or null if end of file reached
     * @throws IOException if there's an error reading the file
     */
    public CsvTranslationRow readNext() throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            currentLineNumber++;
            
            if (line.trim().isEmpty()) {
                continue; // Skip empty lines
            }
            
            try {
                List<String> values = parseCsvLine(line);
                if (values.size() <= Math.max(conceptCodeIndex, translatedTermIndex)) {
                    // Log warning but continue reading
                    continue;
                }
                
                String conceptCode = values.get(conceptCodeIndex).trim();
                String translatedTerm = values.get(translatedTermIndex).trim();
                String comment = commentIndex >= 0 && commentIndex < values.size() ? 
                    values.get(commentIndex).trim() : null;
                
                if (conceptCode.isEmpty() || translatedTerm.isEmpty()) {
                    // Skip rows with empty required fields
                    continue;
                }
                
                return new CsvTranslationRow(conceptCode, translatedTerm, comment);
                
            } catch (Exception e) {
                // Log warning and continue with next row
                continue;
            }
        }
        
        return null; // End of file
    }
    
    /**
     * Reads a batch of translation rows.
     * @param batchSize maximum number of rows to read
     * @return list of translation rows (may be smaller than batchSize if end of file reached)
     * @throws IOException if there's an error reading the file
     */
    public List<CsvTranslationRow> readBatch(int batchSize) throws IOException {
        List<CsvTranslationRow> batch = new ArrayList<>();
        
        for (int i = 0; i < batchSize; i++) {
            CsvTranslationRow row = readNext();
            if (row == null) {
                break; // End of file
            }
            batch.add(row);
        }
        
        return batch;
    }
    
    /**
     * Gets all concept codes from the CSV file for label creation.
     * This method reads through the entire file once to collect concept codes.
     * @return list of concept codes
     * @throws IOException if there's an error reading the file
     */
    public List<String> getAllConceptCodes() throws IOException {
        List<String> conceptCodes = new ArrayList<>();
        CsvTranslationRow row;
        
        while ((row = readNext()) != null) {
            conceptCodes.add(row.getConceptCode());
        }
        
        return conceptCodes;
    }
    
    /**
     * Resets the reader to the beginning of the data (after headers).
     * @throws IOException if there's an error resetting the file
     */
    public void reset() throws IOException {
        if (reader != null) {
            reader.close();
        }
        // Re-open the file and skip the header
        this.reader = new BufferedReader(new FileReader(filePath, StandardCharsets.UTF_8));
        reader.readLine(); // Skip header
        currentLineNumber = 1;
    }
    
    private List<String> parseCsvLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // Handle escaped quotes
                    current.append('"');
                    i++; // Skip next quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                result.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        
        result.add(current.toString());
        return result;
    }
    
    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}