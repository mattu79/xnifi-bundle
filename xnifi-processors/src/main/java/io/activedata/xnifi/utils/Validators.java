package io.activedata.xnifi.utils;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

public class Validators {

	Validator SCRIPTS_VALIDATOR = new Validator() {
		
		@Override
		public ValidationResult validate(String subject, String input, ValidationContext context) {
			return null;
		}
	}; 
}
