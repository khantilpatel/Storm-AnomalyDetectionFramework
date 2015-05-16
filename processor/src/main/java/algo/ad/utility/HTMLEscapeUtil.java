package algo.ad.utility;




/**
* HtmlEscape in Java, which is compatible with 
* @author Ulrich Jensen, http://www.htmlescape.net
* Feel free to get inspired, use or steal this code and use it in your
* own projects.
* License:
* You have the right to use this code in your own project or publish it
* on your own website.
* If you are going to use this code, please include the author lines.
* Use this code at your own risk. The author does not warrent or assume any
* legal liability or responsibility for the accuracy, completeness or usefullness of
* this program code.
*/

public class HTMLEscapeUtil {


 private static char[] hex={'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

 /**
  * Method for html escaping a String, for use in a textarea
  * @param original The String to escape
  * @return The escaped String
  */
 public static String escapeTextArea(String original)
 {
   return escapeSpecial(escapeTags(original));    
 }
 
 /**
  * Normal escape function, for Html escaping Strings
  * @param original The original String
  * @return The escape String
  */
 public static String escape(String original)
 {
   return escapeSpecial(escapeBr(escapeTags(original)));
 }
 
 public static String escapeTags(String original)
 {
   if(original==null) return "";
   StringBuffer out=new StringBuffer("");
   char[] chars=original.toCharArray();
   for(int i=0;i<chars.length;i++)
   {
     boolean found=true;
     switch(chars[i])
     {
       case 60:out.append("&lt;"); break; //<
       case 62:out.append("&gt;"); break; //>
       case 34:out.append("&quot;"); break; //"
       default:found=false;break;
     }
     if(!found) out.append(chars[i]);
     
   }
   return out.toString();
   
 }

 public static String escapeBr(String original)
 {
   if(original==null) return "";
   StringBuffer out=new StringBuffer("");
   char[] chars=original.toCharArray();
   for(int i=0;i<chars.length;i++)
   {
     boolean found=true;
     switch(chars[i])
     {
       case '\n': out.append("<br/>"); break; //newline
       case '\r': break;
       default:found=false;break;
     }
     if(!found) out.append(chars[i]);
     
   }
   return out.toString();
 }
 
 public static String escapeSpecial(String original)
 {
   if(original==null) return "";
   StringBuffer out=new StringBuffer("");
   char[] chars=original.toCharArray();
   for(int i=0;i<chars.length;i++)
   {
       boolean found=true;
     switch(chars[i]) {
       case 38:out.append("&amp;"); break; //&
       case 198:out.append("&AElig;"); break; //�
       case 193:out.append("&Aacute;"); break; //�
       case 194:out.append("&Acirc;"); break; //�
       case 192:out.append("&Agrave;"); break; //�
       case 197:out.append("&Aring;"); break; //�
       case 195:out.append("&Atilde;"); break; //�
       case 196:out.append("&Auml;"); break; //�
       case 199:out.append("&Ccedil;"); break; //�
       case 208:out.append("&ETH;"); break; //�
       case 201:out.append("&Eacute;"); break; //�
       case 202:out.append("&Ecirc;"); break; //�
       case 200:out.append("&Egrave;"); break; //�
       case 203:out.append("&Euml;"); break; //�
       case 205:out.append("&Iacute;"); break; //�
       case 206:out.append("&Icirc;"); break; //�
       case 204:out.append("&Igrave;"); break; //�
       case 207:out.append("&Iuml;"); break; //�
       case 209:out.append("&Ntilde;"); break; //�
       case 211:out.append("&Oacute;"); break; //�
       case 212:out.append("&Ocirc;"); break; //�
       case 210:out.append("&Ograve;"); break; //�
       case 216:out.append("&Oslash;"); break; //�
       case 213:out.append("&Otilde;"); break; //�
       case 214:out.append("&Ouml;"); break; //�
       case 222:out.append("&THORN;"); break; //�
       case 218:out.append("&Uacute;"); break; //�
       case 219:out.append("&Ucirc;"); break; //�
       case 217:out.append("&Ugrave;"); break; //�
       case 220:out.append("&Uuml;"); break; //�
       case 221:out.append("&Yacute;"); break; //�
       case 225:out.append("&aacute;"); break; //�
       case 226:out.append("&acirc;"); break; //�
       case 230:out.append("&aelig;"); break; //�
       case 224:out.append("&agrave;"); break; //�
       case 229:out.append("&aring;"); break; //�
       case 227:out.append("&atilde;"); break; //�
       case 228:out.append("&auml;"); break; //�
       case 231:out.append("&ccedil;"); break; //�
       case 233:out.append("&eacute;"); break; //�
       case 234:out.append("&ecirc;"); break; //�
       case 232:out.append("&egrave;"); break; //�
       case 240:out.append("&eth;"); break; //�
       case 235:out.append("&euml;"); break; //�
       case 237:out.append("&iacute;"); break; //�
       case 238:out.append("&icirc;"); break; //�
       case 236:out.append("&igrave;"); break; //�
       case 239:out.append("&iuml;"); break; //�
       case 241:out.append("&ntilde;"); break; //�
       case 243:out.append("&oacute;"); break; //�
       case 244:out.append("&ocirc;"); break; //�
       case 242:out.append("&ograve;"); break; //�
       case 248:out.append("&oslash;"); break; //�
       case 245:out.append("&otilde;"); break; //�
       case 246:out.append("&ouml;"); break; //�
       case 223:out.append("&szlig;"); break; //�
       case 254:out.append("&thorn;"); break; //�
       case 250:out.append("&uacute;"); break; //�
       case 251:out.append("&ucirc;"); break; //�
       case 249:out.append("&ugrave;"); break; //�
       case 252:out.append("&uuml;"); break; //�
       case 253:out.append("&yacute;"); break; //�
       case 255:out.append("&yuml;"); break; //�
       case 162:out.append("&cent;"); break; //�
       default:
         found=false;
         break;
     }
     if(!found)
     {
       if(chars[i]>127) {
         char c=chars[i];
         int a4=c%16;
         c=(char) (c/16);
         int a3=c%16;
         c=(char) (c/16);
         int a2=c%16;
         c=(char) (c/16);
         int a1=c%16;
         out.append("&#x"+hex[a1]+hex[a2]+hex[a3]+hex[a4]+";");    
       }
       else
       {
         out.append(chars[i]);
       }
     }
   }  
   return out.toString();
 }
 
}
