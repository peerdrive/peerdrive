%% Hotchpotch
%% Copyright (C) 2010  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
%%
%% This program is free software: you can redistribute it and/or modify
%% it under the terms of the GNU General Public License as published by
%% the Free Software Foundation, either version 3 of the License, or
%% (at your option) any later version.
%%
%% This program is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%% GNU General Public License for more details.
%%
%% You should have received a copy of the GNU General Public License
%% along with this program.  If not, see <http://www.gnu.org/licenses/>.

-module(util).
-export([get_time/0, bin_to_hexstr/1, hexstr_to_bin/1, build_path/2, gen_tmp_name/1]).
-export([read_rev/2, read_rev_struct/2, hash_file/1]).
-export([err2int/1, int2err/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Hex conversions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

hex(N) when N < 10 ->
    $0+N;

hex(N) when N >= 10, N < 16 ->
    $a+(N-10).


to_hex(N) when N < 256 ->
    [hex(N div 16), hex(N rem 16)].


list_to_hexstr([]) -> 
    [];

list_to_hexstr([H|T]) ->
    to_hex(H) ++ list_to_hexstr(T).


bin_to_hexstr(Bin) ->
    list_to_hexstr(binary_to_list(Bin)).


hexstr_to_bin(S) ->
	hexstr_to_bin(S, []).


hexstr_to_bin([], Acc) ->
	list_to_binary(lists:reverse(Acc));

hexstr_to_bin([X,Y|T], Acc) ->
	{ok, [V], []} = io_lib:fread("~16u", [X,Y]),
	hexstr_to_bin(T, [V | Acc]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Path utilities
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

build_path(RootPath, Hash) ->
	<<Modulo:8, Body/binary>> = Hash,
	RootPath ++ "/"
	++ to_hex(Modulo) ++ "/"
	++ bin_to_hexstr(Body).


gen_tmp_name(RootPath) ->
	RootPath ++ "/_" ++ util:bin_to_hexstr(crypto:rand_bytes(8)).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Document/Revision reading
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% returns {ok, Data} | {error, Reason}
read_rev(Rev, Part) ->
	case broker:peek(Rev, broker:get_stores([])) of
		{ok, _Errors, Reader} ->
			try
				read_rev_loop(Reader, Part, 0, <<>>)
			after
				broker:close(Reader)
			end;

		{error, Reason, _Errors} ->
			{error, Reason}
	end.


read_rev_loop(Reader, Part, Offset, Acc) ->
	Length = 16#10000,
	case broker:read(Reader, Part, Offset, Length) of
		{ok, _Error, <<>>} ->
			{ok, Acc};
		{ok, _Errors, Data} ->
			read_rev_loop(Reader, Part, Offset+size(Data), <<Acc/binary, Data/binary>>);
		{error, Reason, _Errors} ->
			{error, Reason}
	end.


read_rev_struct(Rev, Part) ->
	case read_rev(Rev, Part) of
		{ok, Data} ->
			case catch struct:decode(Data) of
				{'EXIT', _Reason} ->
					{error, einval};
				Struct ->
					{ok, Struct}
			end;

		{error, Reason} ->
			{error, Reason}
	end.


get_time() ->
	% Thanks to http://www.epochconverter.com/ :)
	calendar:datetime_to_gregorian_seconds(calendar:universal_time()) -
	719528*24*3600.


% returns {ok, Sha1} | {error, Reason}
hash_file(File) ->
	file:position(File, 0),
	hash_file_loop(File, crypto:sha_init()).

hash_file_loop(File, Ctx1) ->
	case file:read(File, 16#100000) of
		{ok, Data} ->
			Ctx2 = crypto:sha_update(Ctx1, Data),
			hash_file_loop(File, Ctx2);
		eof ->
			{ok, binary_part(crypto:sha_final(Ctx1), 0, 16)};
		Else ->
			Else
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Error code encoding/decoding functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Hotchpotch specific codes
err2int(ok) -> 0;
err2int(econflict) -> 1;
err2int(eambig) -> 2;

% Erlang's posix() codes
err2int(e2big) -> 256;
err2int(eacces) -> 257;
err2int(eaddrinuse) -> 258;
err2int(eaddrnotavail) -> 259;
err2int(eadv) -> 260;
err2int(eafnosupport) -> 261;
err2int(eagain) -> 262;
err2int(ealign) -> 263;
err2int(ealready) -> 264;
err2int(ebade) -> 265;
err2int(ebadf) -> 266;
err2int(ebadfd) -> 267;
err2int(ebadmsg) -> 268;
err2int(ebadr) -> 269;
err2int(ebadrpc) -> 270;
err2int(ebadrqc) -> 271;
err2int(ebadslt) -> 272;
err2int(ebfont) -> 273;
err2int(ebusy) -> 274;
err2int(echild) -> 275;
err2int(echrng) -> 276;
err2int(ecomm) -> 277;
err2int(econnaborted) -> 278;
err2int(econnrefused) -> 279;
err2int(econnreset) -> 280;
err2int(edeadlk) -> 281;
err2int(edeadlock) -> 282;
err2int(edestaddrreq) -> 283;
err2int(edirty) -> 284;
err2int(edom) -> 285;
err2int(edotdot) -> 286;
err2int(edquot) -> 287;
err2int(eduppkg) -> 288;
err2int(eexist) -> 289;
err2int(efault) -> 290;
err2int(efbig) -> 291;
err2int(ehostdown) -> 292;
err2int(ehostunreach) -> 293;
err2int(eidrm) -> 294;
err2int(einit) -> 295;
err2int(einprogress) -> 296;
err2int(eintr) -> 297;
err2int(einval) -> 298;
err2int(eio) -> 299;
err2int(eisconn) -> 300;
err2int(eisdir) -> 301;
err2int(eisnam) -> 302;
err2int(elbin) -> 303;
err2int(el2hlt) -> 304;
err2int(el2nsync) -> 305;
err2int(el3hlt) -> 306;
err2int(el3rst) -> 307;
err2int(elibacc) -> 308;
err2int(elibbad) -> 309;
err2int(elibexec) -> 310;
err2int(elibmax) -> 311;
err2int(elibscn) -> 312;
err2int(elnrng) -> 313;
err2int(eloop) -> 314;
err2int(emfile) -> 315;
err2int(emlink) -> 316;
err2int(emsgsize) -> 317;
err2int(emultihop) -> 318;
err2int(enametoolong) -> 319;
err2int(enavail) -> 320;
err2int(enet) -> 321;
err2int(enetdown) -> 322;
err2int(enetreset) -> 323;
err2int(enetunreach) -> 324;
err2int(enfile) -> 325;
err2int(enoano) -> 326;
err2int(enobufs) -> 327;
err2int(enocsi) -> 328;
err2int(enodata) -> 329;
err2int(enodev) -> 330;
err2int(enoent) -> 331;
err2int(enoexec) -> 332;
err2int(enolck) -> 333;
err2int(enolink) -> 334;
err2int(enomem) -> 335;
err2int(enomsg) -> 336;
err2int(enonet) -> 337;
err2int(enopkg) -> 338;
err2int(enoprotoopt) -> 339;
err2int(enospc) -> 340;
err2int(enosr) -> 341;
err2int(enosym) -> 342;
err2int(enosys) -> 343;
err2int(enotblk) -> 344;
err2int(enotconn) -> 345;
err2int(enotdir) -> 346;
err2int(enotempty) -> 347;
err2int(enotnam) -> 348;
err2int(enotsock) -> 349;
err2int(enotsup) -> 350;
err2int(enotty) -> 351;
err2int(enotuniq) -> 352;
err2int(enxio) -> 353;
err2int(eopnotsupp) -> 354;
err2int(eperm) -> 355;
err2int(epfnosupport) -> 356;
err2int(epipe) -> 357;
err2int(eproclim) -> 358;
err2int(eprocunavail) -> 359;
err2int(eprogmismatch) -> 360;
err2int(eprogunavail) -> 361;
err2int(eproto) -> 362;
err2int(eprotonosupport) -> 363;
err2int(eprototype) -> 364;
err2int(erange) -> 365;
err2int(erefused) -> 366;
err2int(eremchg) -> 367;
err2int(eremdev) -> 368;
err2int(eremote) -> 369;
err2int(eremoteio) -> 370;
err2int(eremoterelease) -> 371;
err2int(erofs) -> 372;
err2int(erpcmismatch) -> 373;
err2int(erremote) -> 374;
err2int(eshutdown) -> 375;
err2int(esocktnosupport) -> 376;
err2int(espipe) -> 377;
err2int(esrch) -> 378;
err2int(esrmnt) -> 379;
err2int(estale) -> 380;
err2int(esuccess) -> 381;
err2int(etime) -> 382;
err2int(etimedout) -> 383;
err2int(etoomanyrefs) -> 384;
err2int(etxtbsy) -> 385;
err2int(euclean) -> 386;
err2int(eunatch) -> 387;
err2int(eusers) -> 388;
err2int(eversion) -> 389;
err2int(ewouldblock) -> 390;
err2int(exdev) -> 391;
err2int(exfull) -> 392;
err2int(nxdomain) -> 393;
% Everything else...
err2int(_) -> 16#ffffffff.


int2err(0) -> ok;
int2err(1) -> econflict;
int2err(2) -> eambig;
int2err(256) -> e2big;
int2err(257) -> eacces;
int2err(258) -> eaddrinuse;
int2err(259) -> eaddrnotavail;
int2err(260) -> eadv;
int2err(261) -> eafnosupport;
int2err(262) -> eagain;
int2err(263) -> ealign;
int2err(264) -> ealready;
int2err(265) -> ebade;
int2err(266) -> ebadf;
int2err(267) -> ebadfd;
int2err(268) -> ebadmsg;
int2err(269) -> ebadr;
int2err(270) -> ebadrpc;
int2err(271) -> ebadrqc;
int2err(272) -> ebadslt;
int2err(273) -> ebfont;
int2err(274) -> ebusy;
int2err(275) -> echild;
int2err(276) -> echrng;
int2err(277) -> ecomm;
int2err(278) -> econnaborted;
int2err(279) -> econnrefused;
int2err(280) -> econnreset;
int2err(281) -> edeadlk;
int2err(282) -> edeadlock;
int2err(283) -> edestaddrreq;
int2err(284) -> edirty;
int2err(285) -> edom;
int2err(286) -> edotdot;
int2err(287) -> edquot;
int2err(288) -> eduppkg;
int2err(289) -> eexist;
int2err(290) -> efault;
int2err(291) -> efbig;
int2err(292) -> ehostdown;
int2err(293) -> ehostunreach;
int2err(294) -> eidrm;
int2err(295) -> einit;
int2err(296) -> einprogress;
int2err(297) -> eintr;
int2err(298) -> einval;
int2err(299) -> eio;
int2err(300) -> eisconn;
int2err(301) -> eisdir;
int2err(302) -> eisnam;
int2err(303) -> elbin;
int2err(304) -> el2hlt;
int2err(305) -> el2nsync;
int2err(306) -> el3hlt;
int2err(307) -> el3rst;
int2err(308) -> elibacc;
int2err(309) -> elibbad;
int2err(310) -> elibexec;
int2err(311) -> elibmax;
int2err(312) -> elibscn;
int2err(313) -> elnrng;
int2err(314) -> eloop;
int2err(315) -> emfile;
int2err(316) -> emlink;
int2err(317) -> emsgsize;
int2err(318) -> emultihop;
int2err(319) -> enametoolong;
int2err(320) -> enavail;
int2err(321) -> enet;
int2err(322) -> enetdown;
int2err(323) -> enetreset;
int2err(324) -> enetunreach;
int2err(325) -> enfile;
int2err(326) -> enoano;
int2err(327) -> enobufs;
int2err(328) -> enocsi;
int2err(329) -> enodata;
int2err(330) -> enodev;
int2err(331) -> enoent;
int2err(332) -> enoexec;
int2err(333) -> enolck;
int2err(334) -> enolink;
int2err(335) -> enomem;
int2err(336) -> enomsg;
int2err(337) -> enonet;
int2err(338) -> enopkg;
int2err(339) -> enoprotoopt;
int2err(340) -> enospc;
int2err(341) -> enosr;
int2err(342) -> enosym;
int2err(343) -> enosys;
int2err(344) -> enotblk;
int2err(345) -> enotconn;
int2err(346) -> enotdir;
int2err(347) -> enotempty;
int2err(348) -> enotnam;
int2err(349) -> enotsock;
int2err(350) -> enotsup;
int2err(351) -> enotty;
int2err(352) -> enotuniq;
int2err(353) -> enxio;
int2err(354) -> eopnotsupp;
int2err(355) -> eperm;
int2err(356) -> epfnosupport;
int2err(357) -> epipe;
int2err(358) -> eproclim;
int2err(359) -> eprocunavail;
int2err(360) -> eprogmismatch;
int2err(361) -> eprogunavail;
int2err(362) -> eproto;
int2err(363) -> eprotonosupport;
int2err(364) -> eprototype;
int2err(365) -> erange;
int2err(366) -> erefused;
int2err(367) -> eremchg;
int2err(368) -> eremdev;
int2err(369) -> eremote;
int2err(370) -> eremoteio;
int2err(371) -> eremoterelease;
int2err(372) -> erofs;
int2err(373) -> erpcmismatch;
int2err(374) -> erremote;
int2err(375) -> eshutdown;
int2err(376) -> esocktnosupport;
int2err(377) -> espipe;
int2err(378) -> esrch;
int2err(379) -> esrmnt;
int2err(380) -> estale;
int2err(381) -> esuccess;
int2err(382) -> etime;
int2err(383) -> etimedout;
int2err(384) -> etoomanyrefs;
int2err(385) -> etxtbsy;
int2err(386) -> euclean;
int2err(387) -> eunatch;
int2err(388) -> eusers;
int2err(389) -> eversion;
int2err(390) -> ewouldblock;
int2err(391) -> exdev;
int2err(392) -> exfull;
int2err(393) -> nxdomain;
int2err(_) -> eunknown.

