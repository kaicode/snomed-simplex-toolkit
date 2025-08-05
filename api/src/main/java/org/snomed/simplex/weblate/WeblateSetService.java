package org.snomed.simplex.weblate;

import org.ihtsdo.sso.integration.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snomed.simplex.client.SnowstormClient;
import org.snomed.simplex.client.SnowstormClientFactory;
import org.snomed.simplex.client.domain.CodeSystem;
import org.snomed.simplex.exceptions.ServiceException;
import org.snomed.simplex.exceptions.ServiceExceptionWithStatusCode;
import org.snomed.simplex.service.ServiceHelper;
import org.snomed.simplex.service.SupportRegister;
import org.snomed.simplex.service.TranslationService;
import org.snomed.simplex.service.job.ChangeSummary;
import org.snomed.simplex.service.job.ContentJob;
import org.snomed.simplex.util.CsvStreamReader;
import org.snomed.simplex.util.FileUtils;
import org.snomed.simplex.util.TimerUtil;
import org.snomed.simplex.weblate.domain.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class WeblateSetService {

	private static final String JOB_MESSAGE_USERNAME = "username";
	private static final String JOB_MESSAGE_ID = "id";
	private static final String JOB_TYPE = "type";
	public static final String JOB_TYPE_CREATE = "Create";
	public static final String JOB_TYPE_DELETE = "Delete";

	private final WeblateSetRepository weblateSetRepository;
	private final WeblateClientFactory weblateClientFactory;
	private final SnowstormClientFactory snowstormClientFactory;
	private final TranslationService translationService;
	private final SupportRegister supportRegister;

	private final JmsTemplate jmsTemplate;
	private final String jmsQueuePrefix;
	private final int labelBatchSize;

	private final Map<String, SecurityContext> translationSetUserIdToUserContextMap;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public WeblateSetService(WeblateSetRepository weblateSetRepository, WeblateClientFactory weblateClientFactory, SnowstormClientFactory snowstormClientFactory, TranslationService translationService, SupportRegister supportRegister,
			JmsTemplate jmsTemplate, @Value("${jms.queue.prefix}") String jmsQueuePrefix, @Value("${weblate.label.batch-size}") int labelBatchSize) {

		this.weblateSetRepository = weblateSetRepository;
		this.weblateClientFactory = weblateClientFactory;
		this.snowstormClientFactory = snowstormClientFactory;
		this.translationService = translationService;
		this.supportRegister = supportRegister;
		this.jmsTemplate = jmsTemplate;
		this.jmsQueuePrefix = jmsQueuePrefix;
		this.labelBatchSize = labelBatchSize;
		translationSetUserIdToUserContextMap = new HashMap<>();
	}

	public List<WeblateTranslationSet> findByCodeSystem(String codeSystem) throws ServiceExceptionWithStatusCode {
		List<WeblateTranslationSet> list = weblateSetRepository.findByCodesystem(codeSystem);
		return processTranslationSets(list);
	}

	public List<WeblateTranslationSet> findByCodeSystemAndRefset(String codeSystem, String refsetId) throws ServiceExceptionWithStatusCode {
		List<WeblateTranslationSet> list = weblateSetRepository.findByCodesystemAndRefset(codeSystem, refsetId);
		return processTranslationSets(list);
	}

	public WeblateTranslationSet findSubsetOrThrow(String codeSystem, String refsetId, String label) throws ServiceExceptionWithStatusCode {
		List<WeblateTranslationSet> list = findByCodeSystemAndRefset(codeSystem, refsetId);
		Optional<WeblateTranslationSet> first = list.stream().filter(set -> set.getLabel().equals(label)).findFirst();
		if (first.isEmpty()) {
			throw new ServiceExceptionWithStatusCode("Translation set not found.", HttpStatus.NOT_FOUND);
		}
		return first.get();
	}

	private List<WeblateTranslationSet> processTranslationSets(List<WeblateTranslationSet> list) throws ServiceExceptionWithStatusCode {
		List<WeblateTranslationSet> deleting = list.stream().filter(set -> set.getStatus() == TranslationSetStatus.DELETING).toList();
		List<WeblateTranslationSet> deleted = new ArrayList<>();
		if (!deleting.isEmpty()) {
			WeblateClient weblateClient = weblateClientFactory.getClient();
			for (WeblateTranslationSet set : deleting) {
				WeblateLabel label = weblateClient.getLabel(WeblateClient.COMMON_PROJECT, set.getCompositeLabel());
				if (label == null) {
					weblateSetRepository.delete(set);
					deleted.add(set);
				}
			}
			list = new ArrayList<>(list);
			list.removeAll(deleted);
		}

		String webUrl = weblateClientFactory.getApiUrl().replaceAll("/api/?$", "");
		list.stream()
			.filter(set -> set.getStatus() == TranslationSetStatus.READY)
			.forEach(set -> set.setWeblateLabelUrl("%s/translate/common/snomedct/%s/?q=label:\"%s\""
				.formatted(webUrl, set.getLanguageCodeWithRefsetId(), set.getCompositeLabel())));
		return list;
	}

	public WeblateTranslationSet createSet(WeblateTranslationSet translationSet) throws ServiceExceptionWithStatusCode {
		String codesystemShortName = translationSet.getCodesystem();
		ServiceHelper.requiredParameter("codesystem", codesystemShortName);
		ServiceHelper.requiredParameter("name", translationSet.getName());
		String refsetId = translationSet.getRefset();
		ServiceHelper.requiredParameter("refset", refsetId);
		ServiceHelper.requiredParameter("label", translationSet.getLabel());
		ServiceHelper.requiredParameter("ecl", translationSet.getEcl());
		ServiceHelper.requiredParameter("selectionCodesystem", translationSet.getSelectionCodesystem());

		Optional<WeblateTranslationSet> optional = weblateSetRepository.findByCodesystemAndLabelAndRefset(codesystemShortName, translationSet.getLabel(), refsetId);
		if (optional.isPresent()) {
			throw new ServiceExceptionWithStatusCode("A translation set with this label already exists.", HttpStatus.CONFLICT);
		}

		CodeSystem codeSystem = snowstormClientFactory.getClient().getCodeSystemOrThrow(codesystemShortName);
		String languageCode = codeSystem.getTranslationLanguages().get(refsetId);
		if (languageCode == null) {
			throw new ServiceExceptionWithStatusCode("Language code not found for refset: " + refsetId, HttpStatus.NOT_FOUND);
		}
		translationSet.setLanguageCode(languageCode);

		WeblateClient weblateClient = weblateClientFactory.getClient();
		if (!weblateClient.isTranslationExistsSearchByLanguageRefset(translationSet.getLanguageCodeWithRefsetId())) {
			throw new ServiceExceptionWithStatusCode("Translation does not exist in Weblate, " +
					"please start language initialisation job or wait for it to finish.", HttpStatus.CONFLICT);
		}

		translationSet.setStatus(TranslationSetStatus.INITIALISING);

		logger.info("Queueing Weblate Translation Set for creation {}/{}/{}", codesystemShortName, refsetId, translationSet.getLabel());
		weblateSetRepository.save(translationSet);
		String username = SecurityUtil.getUsername();
		translationSetUserIdToUserContextMap.put(username, SecurityContextHolder.getContext());

		jmsTemplate.convertAndSend(jmsQueuePrefix + ".translation-set.processing",
			Map.of(JOB_TYPE, JOB_TYPE_CREATE,
				JOB_MESSAGE_USERNAME, username,
				JOB_MESSAGE_ID, translationSet.getId()));
		return translationSet;
	}

	public WeblateTranslationSet createSetFromCsv(String codesystemShortName, String refsetId, String name, String label,
			InputStream csvInputStream, String conceptCodeColumn, String translatedTermColumn, String commentColumn) 
			throws ServiceExceptionWithStatusCode {
		
		ServiceHelper.requiredParameter("codesystem", codesystemShortName);
		ServiceHelper.requiredParameter("name", name);
		ServiceHelper.requiredParameter("refset", refsetId);
		ServiceHelper.requiredParameter("label", label);
		ServiceHelper.requiredParameter("conceptCodeColumn", conceptCodeColumn);
		ServiceHelper.requiredParameter("translatedTermColumn", translatedTermColumn);

		// Check if translation set with this label already exists
		Optional<WeblateTranslationSet> optional = weblateSetRepository.findByCodesystemAndLabelAndRefset(codesystemShortName, label, refsetId);
		if (optional.isPresent()) {
			throw new ServiceExceptionWithStatusCode("A translation set with this label already exists.", HttpStatus.CONFLICT);
		}

		// Get the code system and language code
		CodeSystem codeSystem = snowstormClientFactory.getClient().getCodeSystemOrThrow(codesystemShortName);
		String languageCode = codeSystem.getTranslationLanguages().get(refsetId);
		if (languageCode == null) {
			throw new ServiceExceptionWithStatusCode("Language code not found for refset: " + refsetId, HttpStatus.NOT_FOUND);
		}

		// Verify Weblate translation exists
		WeblateClient weblateClient = weblateClientFactory.getClient();
		String languageCodeWithRefsetId = languageCode + "-" + refsetId;
		if (!weblateClient.isTranslationExistsSearchByLanguageRefset(languageCodeWithRefsetId)) {
			throw new ServiceExceptionWithStatusCode("Translation does not exist in Weblate, " +
					"please start language initialisation job or wait for it to finish.", HttpStatus.CONFLICT);
		}

		// Save CSV to temporary file and validate content
		String tempFilePath;
		int rowCount;
		try {
			tempFilePath = saveCsvToTempFile(csvInputStream);
			rowCount = validateCsvFile(tempFilePath, conceptCodeColumn, translatedTermColumn, commentColumn);
		} catch (IOException e) {
			throw new ServiceExceptionWithStatusCode("Failed to process CSV file: " + e.getMessage(), HttpStatus.BAD_REQUEST);
		}

		if (rowCount == 0) {
			// Clean up temp file
			try {
				Files.deleteIfExists(Path.of(tempFilePath));
			} catch (IOException e) {
				logger.warn("Failed to delete temporary CSV file: {}", tempFilePath, e);
			}
			throw new ServiceExceptionWithStatusCode("CSV file contains no valid translation rows.", HttpStatus.BAD_REQUEST);
		}

		// Create the translation set with CSV subset type
		WeblateTranslationSet translationSet = new WeblateTranslationSet(codesystemShortName, refsetId, name, label,
			null, TranslationSubsetType.CSV, codesystemShortName);
		translationSet.setLanguageCode(languageCode);
		translationSet.setStatus(TranslationSetStatus.INITIALISING);

		// Store the temporary file path and column mappings for later processing
		translationSet.setCsvTempFilePath(tempFilePath);
		translationSet.setConceptCodeColumn(conceptCodeColumn);
		translationSet.setTranslatedTermColumn(translatedTermColumn);
		translationSet.setCommentColumn(commentColumn);

		logger.info("Queueing Weblate Translation Set for CSV creation {}/{}/{} with {} translations from temp file {}", 
			codesystemShortName, refsetId, label, rowCount, tempFilePath);
		
		weblateSetRepository.save(translationSet);
		String username = SecurityUtil.getUsername();
		translationSetUserIdToUserContextMap.put(username, SecurityContextHolder.getContext());

		jmsTemplate.convertAndSend(jmsQueuePrefix + ".translation-set.processing",
			Map.of(JOB_TYPE, JOB_TYPE_CREATE,
				JOB_MESSAGE_USERNAME, username,
				JOB_MESSAGE_ID, translationSet.getId()));
		
		return translationSet;
	}

	private String saveCsvToTempFile(InputStream csvInputStream) throws IOException {
		// Create temporary file
		Path tempFile = Files.createTempFile("weblate-csv-", ".csv");
		
		// Copy input stream to temporary file
		try (InputStream input = csvInputStream) {
			Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
		}
		
		logger.info("Saved CSV to temporary file: {}", tempFile.toString());
		return tempFile.toString();
	}

	private int validateCsvFile(String tempFilePath, String conceptCodeColumn, String translatedTermColumn, String commentColumn) 
			throws IOException {
		
		int validRowCount = 0;
		
		try (CsvStreamReader csvReader = new CsvStreamReader(tempFilePath, conceptCodeColumn, translatedTermColumn, commentColumn)) {
			// Count valid rows by reading through the file
			while (csvReader.readNext() != null) {
				validRowCount++;
			}
		}
		
		logger.info("Validated CSV file {} with {} valid translation rows", tempFilePath, validRowCount);
		return validRowCount;
	}

	public WeblatePage<WeblateUnit> getSampleRows(WeblateTranslationSet translationSet) throws ServiceExceptionWithStatusCode {
		WeblateClient weblateClient = weblateClientFactory.getClient();
		return weblateClient.getUnitPage(UnitQueryBuilder.of(WeblateClient.COMMON_PROJECT, WeblateClient.SNOMEDCT_COMPONENT)
			.languageCode(translationSet.getLanguageCodeWithRefsetId())
			.compositeLabel(translationSet.getCompositeLabel())
			.pageSize(10));
	}

	public int getStateCount(WeblateTranslationSet translationSet, String state) throws ServiceExceptionWithStatusCode {
		WeblateClient weblateClient = weblateClientFactory.getClient();
		WeblatePage<WeblateUnit> unitPage = weblateClient.getUnitPage(UnitQueryBuilder.of(WeblateClient.COMMON_PROJECT, WeblateClient.SNOMEDCT_COMPONENT)
			.languageCode(translationSet.getLanguageCodeWithRefsetId())
			.compositeLabel(translationSet.getCompositeLabel())
			.state(state)
			.pageSize(1));
		return unitPage.count();
	}

	public void deleteSet(WeblateTranslationSet translationSet) {
		logger.info("Queueing Weblate Translation Set for deletion {}/{}/{}", translationSet.getCodesystem(), translationSet.getRefset(), translationSet.getLabel());
		translationSet.setStatus(TranslationSetStatus.DELETING);
		weblateSetRepository.save(translationSet);

		String username = SecurityUtil.getUsername();
		translationSetUserIdToUserContextMap.put(username, SecurityContextHolder.getContext());

		jmsTemplate.convertAndSend(jmsQueuePrefix + ".translation-set.processing",
			Map.of(JOB_TYPE, JOB_TYPE_DELETE,
				JOB_MESSAGE_USERNAME, username,
				JOB_MESSAGE_ID, translationSet.getId()));

	}

	@JmsListener(destination = "${jms.queue.prefix}.translation-set.processing", concurrency = "1")
	public void processTranslationSet(Map<String, String> jobMessage) {
		String username = jobMessage.get(JOB_MESSAGE_USERNAME);
		String translationSetId = jobMessage.get(JOB_MESSAGE_ID);
		String jobType = jobMessage.get(JOB_TYPE);
		Optional<WeblateTranslationSet> optional = weblateSetRepository.findById(translationSetId);
		if (optional.isEmpty()) {
			logger.info("Weblate set was deleted before being processed {}", translationSetId);
			return;
		}

		WeblateTranslationSet translationSet = optional.get();
		SecurityContextHolder.setContext(translationSetUserIdToUserContextMap.get(username));

		try {
			logger.info("Starting - {} translation Set: {}/{}/{}",
				jobType, translationSet.getCodesystem(), translationSet.getRefset(), translationSet.getLabel());

			WeblateClient weblateClient = weblateClientFactory.getClient();
			SnowstormClient snowstormClient = snowstormClientFactory.getClient();
			if (jobType.equals(JOB_TYPE_CREATE)) {
				doCreateSet(translationSet, weblateClient, snowstormClient);
			} else if (jobType.equals(JOB_TYPE_DELETE)) {
				doDeleteSet(translationSet, weblateClient);
			} else {
				String errorMessage = "Unrecognised job type: %s, translationSet: %s, username: %s".formatted(jobType, translationSetId, username);
				supportRegister.handleSystemError(CodeSystem.SHARED, errorMessage, new ServiceException(errorMessage));
				return;
			}

			logger.info("Success - {} translation set: {}/{}/{}",
				jobType, translationSet.getCodesystem(), translationSet.getRefset(), translationSet.getLabel());

		} catch (Exception e) {
			logger.error("Error - {} translation set: {}/{}/{}",
				jobType, translationSet.getCodesystem(), translationSet.getRefset(), translationSet.getLabel(), e);

			// Update status to failed
			translationSet.setStatus(TranslationSetStatus.FAILED);
			weblateSetRepository.save(translationSet);
		} finally {
			SecurityContextHolder.clearContext();
		}
	}

	public ChangeSummary pullTranslationSubset(ContentJob contentJob, String label) throws ServiceException {
		WeblateClient weblateClient = weblateClientFactory.getClient();
		CodeSystem codeSystem = contentJob.getCodeSystemObject();
		WeblateTranslationSet translationSet = findSubsetOrThrow(codeSystem.getShortName(), contentJob.getRefsetId(), label);
		File subsetFile;
		try {
			subsetFile = weblateClient.downloadTranslationSubset(translationSet);
		} catch (IOException e) {
			throw new ServiceExceptionWithStatusCode("Failed to download translation file from Weblate.", HttpStatus.INTERNAL_SERVER_ERROR, e);
		}
		try (FileInputStream fileInputStream = new FileInputStream(subsetFile)) {
			contentJob.addUpload(fileInputStream, "weblate-automatic-download.csv");
			return translationService.uploadTranslationAsWeblateCSV(true, contentJob);
		} catch (IOException e) {
			throw new ServiceExceptionWithStatusCode("Translation upload step failed.", HttpStatus.INTERNAL_SERVER_ERROR, e);
		} finally {
			FileUtils.deleteOrLogWarning(subsetFile);
		}
	}

	private void doCreateSet(WeblateTranslationSet translationSet, WeblateClient weblateClient, SnowstormClient snowstormClient) throws ServiceExceptionWithStatusCode {
		TimerUtil timerUtil = new TimerUtil("Adding label %s".formatted(translationSet.getLabel()));
		// Update status to processing
		translationSet.setStatus(TranslationSetStatus.PROCESSING);
		weblateSetRepository.save(translationSet);

		String compositeLabel = translationSet.getCompositeLabel();
		WeblateLabel weblateLabel = weblateClient.getCreateLabel(WeblateClient.COMMON_PROJECT, compositeLabel);

		if (translationSet.getSubsetType() == TranslationSubsetType.CSV) {
			// Handle CSV translation set creation
			doCreateSetFromCsv(translationSet, weblateClient, weblateLabel, timerUtil);
		} else {
			// Handle ECL-based translation set creation
			doCreateSetFromEcl(translationSet, weblateClient, snowstormClient, weblateLabel, timerUtil);
		}

		timerUtil.finish();
		translationSet.setStatus(TranslationSetStatus.READY);
		weblateSetRepository.save(translationSet);
	}

	private void doCreateSetFromEcl(WeblateTranslationSet translationSet, WeblateClient weblateClient, 
			SnowstormClient snowstormClient, WeblateLabel weblateLabel, TimerUtil timerUtil) throws ServiceExceptionWithStatusCode {
		
		String selectionCodesystemName = translationSet.getSelectionCodesystem();
		CodeSystem selectionCodeSystem = snowstormClient.getCodeSystemOrThrow(selectionCodesystemName);
		SnowstormClient.ConceptIdStream conceptIdStream = snowstormClient.getConceptIdStream(selectionCodeSystem.getBranchPath(), translationSet.getEcl());

		String code;
		String compositeLabel = translationSet.getCompositeLabel();
		List<String> codes = new ArrayList<>();
		int done = 0;
		
		while ((code = conceptIdStream.get()) != null) {
			codes.add(code);
			if (codes.size() == labelBatchSize) {
				bulkAddLabelsToBatch(compositeLabel, codes, weblateClient, weblateLabel);
				timerUtil.checkpoint("Completed batch");
				done += labelBatchSize;
				updateProcessingTotal(translationSet, done, conceptIdStream.getTotal());
			}
		}
		if (!codes.isEmpty()) {
			bulkAddLabelsToBatch(compositeLabel, codes, weblateClient, weblateLabel);
			updateProcessingTotal(translationSet, conceptIdStream.getTotal(), conceptIdStream.getTotal());
		}
	}

	private void doCreateSetFromCsv(WeblateTranslationSet translationSet, WeblateClient weblateClient, 
			WeblateLabel weblateLabel, TimerUtil timerUtil) throws ServiceExceptionWithStatusCode {
		
		String tempFilePath = translationSet.getCsvTempFilePath();
		if (tempFilePath == null || tempFilePath.isEmpty()) {
			throw new ServiceExceptionWithStatusCode("No CSV temp file path found for translation set", HttpStatus.BAD_REQUEST);
		}

		String compositeLabel = translationSet.getCompositeLabel();
		
		try {
			// First pass: collect all concept codes for label creation
			List<String> allConceptCodes = new ArrayList<>();
			try (CsvStreamReader csvReader = new CsvStreamReader(tempFilePath, 
					translationSet.getConceptCodeColumn(), 
					translationSet.getTranslatedTermColumn(), 
					translationSet.getCommentColumn())) {
				
				CsvTranslationRow row;
				while ((row = csvReader.readNext()) != null) {
					allConceptCodes.add(row.getConceptCode());
				}
			}

			// Process concept codes in batches for label creation
			int done = 0;
			int total = allConceptCodes.size();
			
			for (int i = 0; i < allConceptCodes.size(); i += labelBatchSize) {
				int endIndex = Math.min(i + labelBatchSize, allConceptCodes.size());
				List<String> batch = allConceptCodes.subList(i, endIndex);
				
				bulkAddLabelsToBatch(compositeLabel, batch, weblateClient, weblateLabel);
				timerUtil.checkpoint("Completed CSV label batch");
				done += batch.size();
				updateProcessingTotal(translationSet, done, total);
			}

			// Second pass: apply translations using streaming
			applyCsvTranslationsStreaming(translationSet, tempFilePath);
			
		} catch (IOException e) {
			throw new ServiceExceptionWithStatusCode("Failed to process CSV file: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			logger.error("Failed to apply CSV translations for set {}/{}/{}", 
				translationSet.getCodesystem(), translationSet.getRefset(), translationSet.getLabel(), e);
			// Continue with the set creation even if translations fail to apply
			// The set will be created and translations can be applied later
		} finally {
			// Clean up temporary file
			cleanupTempFile(tempFilePath);
		}
	}

	private void applyCsvTranslationsStreaming(WeblateTranslationSet translationSet, String tempFilePath) 
			throws ServiceException, IOException {
		
		// Create a temporary file for the translation service CSV format
		Path translationCsvFile = Files.createTempFile("weblate-translation-", ".csv");
		
		try {
			boolean hasComments = false;
			
			// First pass: check if any row has comments to determine if we need the note column
			try (CsvStreamReader csvReader = new CsvStreamReader(tempFilePath, 
					translationSet.getConceptCodeColumn(), 
					translationSet.getTranslatedTermColumn(), 
					translationSet.getCommentColumn())) {
				
				List<CsvTranslationRow> sampleRows = csvReader.readBatch(100);
				for (CsvTranslationRow row : sampleRows) {
					if (row.getComment() != null && !row.getComment().isEmpty()) {
						hasComments = true;
						break;
					}
				}
			}
			
			// Second pass: write CSV in the format expected by the translation service
			try (CsvStreamReader csvReader = new CsvStreamReader(tempFilePath, 
					translationSet.getConceptCodeColumn(), 
					translationSet.getTranslatedTermColumn(), 
					translationSet.getCommentColumn());
				 BufferedWriter writer = Files.newBufferedWriter(translationCsvFile, StandardCharsets.UTF_8)) {
				
				// Write header
				if (hasComments) {
					writer.write("context,source,target,note\n");
				} else {
					writer.write("context,source,target\n");
				}
				
				// Write all rows
				CsvTranslationRow row;
				while ((row = csvReader.readNext()) != null) {
					writeTranslationRow(writer, row, hasComments);
				}
			}
			
			// Apply translations using the existing translation service
			try (InputStream csvInputStream = Files.newInputStream(translationCsvFile)) {
				CodeSystem codeSystem = snowstormClientFactory.getClient().getCodeSystemOrThrow(translationSet.getCodesystem());
				
				ContentJob contentJob = new ContentJob();
				contentJob.setRefsetId(translationSet.getRefset());
				contentJob.setCodeSystemObject(codeSystem);
				contentJob.addUpload(csvInputStream, "csv-translations.csv");
				
				translationService.uploadTranslationAsWeblateCSV(true, contentJob);
				logger.info("Successfully applied CSV translations for set {}/{}/{}", 
					translationSet.getCodesystem(), translationSet.getRefset(), translationSet.getLabel());
			}
			
		} finally {
			// Clean up the temporary translation CSV file
			try {
				Files.deleteIfExists(translationCsvFile);
			} catch (IOException e) {
				logger.warn("Failed to delete temporary translation CSV file: {}", translationCsvFile, e);
			}
		}
	}

	private void writeTranslationRow(BufferedWriter writer, CsvTranslationRow row, boolean hasComments) throws IOException {
		// Escape quotes in CSV values
		String conceptCode = row.getConceptCode();
		String translatedTerm = row.getTranslatedTerm().replace("\"", "\"\"");
		String comment = row.getComment();
		
		writer.write(String.format("%s,\"%s\",\"%s\"", conceptCode, conceptCode, translatedTerm));
		
		if (hasComments) {
			if (comment != null && !comment.isEmpty()) {
				writer.write(",\"" + comment.replace("\"", "\"\"") + "\"");
			} else {
				writer.write(",\"\"");
			}
		}
		writer.write("\n");
	}

	private void cleanupTempFile(String tempFilePath) {
		if (tempFilePath != null) {
			try {
				Files.deleteIfExists(Path.of(tempFilePath));
				logger.info("Cleaned up temporary CSV file: {}", tempFilePath);
			} catch (IOException e) {
				logger.warn("Failed to delete temporary CSV file: {}", tempFilePath, e);
			}
		}
	}

	private void updateProcessingTotal(WeblateTranslationSet translationSet, int done, int total) {
		if (total == 0) {
			total++;
		}
		translationSet.setPercentageProcessed((int) (((float) done / (float) total) * 100));
		translationSet.setSize(total);
		weblateSetRepository.save(translationSet);
	}

	private void doDeleteSet(WeblateTranslationSet translationSet, WeblateClient weblateClient) {
		weblateClient.deleteLabelAsync(WeblateClient.COMMON_PROJECT, translationSet.getCompositeLabel());
	}

	private void bulkAddLabelsToBatch(String label, List<String> codes, WeblateClient weblateClient, WeblateLabel weblateLabel) throws ServiceExceptionWithStatusCode {
		logger.info("Adding batch of label:{} to {} units", label, codes.size());
		weblateClient.bulkAddLabels(WeblateClient.COMMON_PROJECT, weblateLabel.id(), codes);
		logger.info("Added label batch");
		codes.clear();
	}
}
