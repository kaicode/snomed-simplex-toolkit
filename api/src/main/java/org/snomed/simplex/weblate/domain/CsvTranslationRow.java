package org.snomed.simplex.weblate.domain;

/**
 * Represents a single translation row from a CSV file.
 */
public class CsvTranslationRow {
    
    private String conceptCode;
    private String translatedTerm;
    private String comment;
    
    public CsvTranslationRow() {
    }
    
    public CsvTranslationRow(String conceptCode, String translatedTerm, String comment) {
        this.conceptCode = conceptCode;
        this.translatedTerm = translatedTerm;
        this.comment = comment;
    }
    
    public String getConceptCode() {
        return conceptCode;
    }
    
    public void setConceptCode(String conceptCode) {
        this.conceptCode = conceptCode;
    }
    
    public String getTranslatedTerm() {
        return translatedTerm;
    }
    
    public void setTranslatedTerm(String translatedTerm) {
        this.translatedTerm = translatedTerm;
    }
    
    public String getComment() {
        return comment;
    }
    
    public void setComment(String comment) {
        this.comment = comment;
    }
    
    @Override
    public String toString() {
        return "CsvTranslationRow{" +
                "conceptCode='" + conceptCode + '\'' +
                ", translatedTerm='" + translatedTerm + '\'' +
                ", comment='" + comment + '\'' +
                '}';
    }
}