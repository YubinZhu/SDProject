package oldApp.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Created by yubzhu on 2019/6/9
 */

public class LogService {

    private Logger logger;

    public LogService(Class<?> clazz) {
        logger = LoggerFactory.getLogger(clazz);
    }

    private static String getRequestURL(HttpServletRequest httpServletRequest) throws UnsupportedEncodingException {
        if (httpServletRequest.getQueryString() == null) {
            return httpServletRequest.getRequestURL().toString();
        } else {
            return httpServletRequest.getRequestURL() + "?" + URLDecoder.decode(httpServletRequest.getQueryString(), "utf-8");
        }
    }

    public void printQueryOkInfo(HttpServletRequest httpServletRequest) {
        try {
            String url = getRequestURL(httpServletRequest);
            logger.info("At [{}] --- Query ok.", url);
        } catch (UnsupportedEncodingException e) {
            logger.error("FATAL ERROR --- Exception \"{}\" occurred.", UnsupportedEncodingException.class.getName());
        }
    }

    public void printExceptionOccurredWarning(HttpServletRequest httpServletRequest, Exception exception) {
        try {
            String url = getRequestURL(httpServletRequest);
            logger.warn("At [{}] --- Exception \"{}\" occurred.", url, exception.getClass().getName());
        } catch (UnsupportedEncodingException e) {
            logger.error("FATAL ERROR --- Exception \"{}\" occurred.", UnsupportedEncodingException.class.getName());
        }
    }

}
