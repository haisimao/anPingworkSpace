function stringToHex(str) { 
	var val = "";
	for (var i = 0; i < str.length; i++) {
		if (val == "") 
			val = str.charCodeAt(i).toString(16);
		else
			val += str.charCodeAt(i).toString(16);
		}
		return val;
	}

function YunSuoAutoJump() {
	var width = screen.width;
	var height = screen.height;
	var screendate = width + "," + height;
	// var curlocation = window.location.href;
	// if (-1 == curlocation.indexOf("security_verify_")) {
	// 	document.cookie = "srcurl=" + stringToHex(window.location.href) + ";path=/;";
	// }
	// self.location = "/?security_verify_data=" + stringToHex(screendate);
	console.log(stringToHex(screendate))
}

YunSuoAutoJump()